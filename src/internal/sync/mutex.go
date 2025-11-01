// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sync provides basic synchronization primitives such as mutual
// exclusion locks to internal packages (including ones that depend on sync).
//
// Tests are defined in package [sync].
package sync

import (
	"internal/race"
	"sync/atomic"
	"unsafe"
)

// A Mutex is a mutual exclusion lock.
//
// See package [sync.Mutex] documentation.
type Mutex struct {
	// Состояние мьютекса закодировано в битах: 29 на waiter и старшие 3 для Locked, Woken, Starving.
	// 000..00 - 29 битов для отслеживания сколько горутин ожидают мьютекс и 000 для состояний мьютекса.
	// Рантаймовый семафор хранит очередь ожидающих горутин,
	// но 29 битов в state позволяет принимать быстрые решения без похода в рантайм.
	// Также state поддерживает атомарные операции и в lockSlow() проще менять данные через него.
	state int32
	sema  uint32 // Ручка для парковки/разбудки горутин через рантаймовый семафор.
}

// Нюанс №1: Мьютексы не хранят владельца блокировки, блокировку можно снять другой горутиной. В отличии от мьютекса ОС Linux, например.

const (
	// Мьютекс сейчас захвачен.
	mutexLocked      = 1 << iota // mutex is locked
	// Какую-то горутину уже будят, будить остальные не нужно.
	mutexWoken
	// Мьютекс голодает.
	// Это специальный режим, который позволяет горутинам не голодать и включается если горутина ждала дольше 1ms.
	mutexStarving
	//
	mutexWaiterShift = iota

	// То есть:
	// бит 0 (mutexLocked=1) - устанавливается в 1, если мьютекс заблокирован в данный момент.
	// бит 1 (mutexWoken=2) - устанавливается в 1, если какая-либо горутина была разбужена и пытается захватить мьютекс. Остальные будить не нужно.
	// бит 2 (mutexStarving=4) - устанавливается в 1, если мьютекс голодает .
	// старшие биты (сдвиг mutexWaiterShift == 3) - отслеживают количество ожидающих горутин.

	// Mutex fairness.
	//
	// Mutex can be in 2 modes of operations: normal and starvation.
	// In normal mode waiters are queued in FIFO order, but a woken up waiter
	// does not own the mutex and competes with new arriving goroutines over
	// the ownership. New arriving goroutines have an advantage -- they are
	// already running on CPU and there can be lots of them, so a woken up
	// waiter has good chances of losing. In such case it is queued at front
	// of the wait queue. If a waiter fails to acquire the mutex for more than 1ms,
	// it switches mutex to the starvation mode.
	//
	// In starvation mode ownership of the mutex is directly handed off from
	// the unlocking goroutine to the waiter at the front of the queue.
	// New arriving goroutines don't try to acquire the mutex even if it appears
	// to be unlocked, and don't try to spin. Instead they queue themselves at
	// the tail of the wait queue.
	//
	// If a waiter receives ownership of the mutex and sees that either
	// (1) it is the last waiter in the queue, or (2) it waited for less than 1 ms,
	// it switches mutex back to normal operation mode.
	//
	// Normal mode has considerably better performance as a goroutine can acquire
	// a mutex several times in a row even if there are blocked waiters.
	// Starvation mode is important to prevent pathological cases of tail latency.
	starvationThresholdNs = 1e6
)

// Lock locks m.
//
// See package [sync.Mutex] documentation.
func (m *Mutex) Lock() {
	// Fast path: grab unlocked mutex.
	// CAS - операция обновления значения с A на B, которая выполняется если в ячейке лежит A = <что-то>.
	// Здесь мы пытаемся захватить свободный мьютекс.
	if atomic.CompareAndSwapInt32(&m.state, 0, mutexLocked) {
		// Штука для race detector.
		if race.Enabled {
			race.Acquire(unsafe.Pointer(m))
		}
		return
	}
	// Slow path (outlined so that the fast path can be inlined)
	// Если CAS не прошёл - мьютекс кто-то держит или он голодает.
	m.lockSlow()
}

// TryLock tries to lock m and reports whether it succeeded.
//
// See package [sync.Mutex] documentation.
// Чуть более быстрый, но менее справедливый метод взятия блокировки,
// способный "украсть" её у горутины, которую начали будить.
// Это best effort-операция.
func (m *Mutex) TryLock() bool {
	old := m.state
	if old&(mutexLocked|mutexStarving) != 0 {
		return false
	}

	// There may be a goroutine waiting for the mutex, but we are
	// running now and can try to grab the mutex before that
	// goroutine wakes up.
	if !atomic.CompareAndSwapInt32(&m.state, old, old|mutexLocked) {
		return false
	}

	if race.Enabled {
		race.Acquire(unsafe.Pointer(m))
	}
	return true
}

func (m *Mutex) lockSlow() {
	// Это поля значений горутины, которые нужны для выбора стратегии lockSlow().

	var waitStartTime int64 // Момент времени, в который горутина впервые уснула на семафоре.
	starving := false // Голодает ли горутина?
	awoke := false    // Будили ли горутину?
	iter := 0         // Сколько раз горутина уже крутилась на этой попытке??
	old := m.state    // Снимок состояния.
	for {
		// Don't spin in starvation mode, ownership is handed off to waiters
		// so we won't be able to acquire the mutex anyway.
		if old&(mutexLocked|mutexStarving) == mutexLocked && runtime_canSpin(iter) {
			// Active spinning makes sense.
			// Try to set mutexWoken flag to inform Unlock
			// to not wake other blocked goroutines.
			if !awoke && old&mutexWoken == 0 && old>>mutexWaiterShift != 0 &&
				atomic.CompareAndSwapInt32(&m.state, old, old|mutexWoken) {
				awoke = true
			}
			runtime_doSpin()
			iter++
			old = m.state
			continue
		}
		new := old
		// Don't try to acquire starving mutex, new arriving goroutines must queue.
		if old&mutexStarving == 0 {
			new |= mutexLocked
		}
		if old&(mutexLocked|mutexStarving) != 0 {
			new += 1 << mutexWaiterShift
		}
		// The current goroutine switches mutex to starvation mode.
		// But if the mutex is currently unlocked, don't do the switch.
		// Unlock expects that starving mutex has waiters, which will not
		// be true in this case.
		if starving && old&mutexLocked != 0 {
			new |= mutexStarving
		}
		if awoke {
			// The goroutine has been woken from sleep,
			// so we need to reset the flag in either case.
			if new&mutexWoken == 0 {
				throw("sync: inconsistent mutex state")
			}
			new &^= mutexWoken
		}
		if atomic.CompareAndSwapInt32(&m.state, old, new) {
			if old&(mutexLocked|mutexStarving) == 0 {
				break // locked the mutex with CAS
			}
			// If we were already waiting before, queue at the front of the queue.
			queueLifo := waitStartTime != 0
			if waitStartTime == 0 {
				waitStartTime = runtime_nanotime()
			}
			runtime_SemacquireMutex(&m.sema, queueLifo, 2)

			// Проставляем режим голодания если ждём больше 1ms.
			starving = starving || runtime_nanotime()-waitStartTime > starvationThresholdNs
			old = m.state
			if old&mutexStarving != 0 {
				// If this goroutine was woken and mutex is in starvation mode,
				// ownership was handed off to us but mutex is in somewhat
				// inconsistent state: mutexLocked is not set and we are still
				// accounted as waiter. Fix that.
				if old&(mutexLocked|mutexWoken) != 0 || old>>mutexWaiterShift == 0 {
					throw("sync: inconsistent mutex state")
				}
				delta := int32(mutexLocked - 1<<mutexWaiterShift)
				if !starving || old>>mutexWaiterShift == 1 {
					// Exit starvation mode.
					// Critical to do it here and consider wait time.
					// Starvation mode is so inefficient, that two goroutines
					// can go lock-step infinitely once they switch mutex
					// to starvation mode.
					delta -= mutexStarving
				}
				atomic.AddInt32(&m.state, delta)
				break
			}
			awoke = true
			iter = 0
		} else {
			old = m.state
		}
	}

	if race.Enabled {
		race.Acquire(unsafe.Pointer(m))
	}
}

// Unlock unlocks m.
//
// See package [sync.Mutex] documentation.
func (m *Mutex) Unlock() {
	if race.Enabled {
		_ = m.state
		race.Release(unsafe.Pointer(m))
	}

	// Fast path: drop lock bit.
	// -mutexLocked = -1, это позволяет снять бит "мьютекс заблокирован", не затронув остальные биты.
	new := atomic.AddInt32(&m.state, -mutexLocked)

	// Если после снятия блокировки остались ненулевые биты - надо разбираться.
	// Например в waiters кто-то остался || было голодание || кого-то будят // всё вместе.
	if new != 0 {
		// Outlined slow path to allow inlining the fast path.
		// To hide unlockSlow during tracing we skip one extra frame when tracing GoUnblock.
		m.unlockSlow(new)
	}
}

// Отвечает за 2 сценария: мьютекс в обычном режиме и мьютекс в режие голодания.
func (m *Mutex) unlockSlow(new int32) {
	// Анлок мьютекса без блокировки -> паника.
	if (new+mutexLocked)&mutexLocked == 0 {
		fatal("sync: unlock of unlocked mutex")
	}

	// Сценарий мьютекса в нормальном режиме.
	if new&mutexStarving == 0 {
		old := new
		// Цикл чтобы eventually выиграть CAS.
		for {
			// If there are no waiters or a goroutine has already
			// been woken or grabbed the lock, no need to wake anyone.
			// In starvation mode ownership is directly handed off from unlocking
			// goroutine to the next waiter. We are not part of this chain,
			// since we did not observe mutexStarving when we unlocked the mutex above.
			// So get off the way.

			// Нет waiters || (кто-то заново взял блокировку || кого-то будят || включился режим голодания) -> выходим.
			if old>>mutexWaiterShift == 0 || old&(mutexLocked|mutexWoken|mutexStarving) != 0 {
				return
			}
			// Grab the right to wake someone.
			// Уменьшаем счётчик waiters на 1 и ставим бит Woken в 1.
			new = (old - 1<<mutexWaiterShift) | mutexWoken
			if atomic.CompareAndSwapInt32(&m.state, old, new) {
				// Будим горутину через рантаймовый семафор, false -> пробуждение не в голодании и без handoff.
				runtime_Semrelease(&m.sema, false, 2)
				return
			}
			old = m.state
		}
	} else {
		// Starving mode: handoff mutex ownership to the next waiter, and yield
		// our time slice so that the next waiter can start to run immediately.
		// Note: mutexLocked is not set, the waiter will set it after wakeup.
		// But mutex is still considered locked if mutexStarving is set,
		// so new coming goroutines won't acquire it.

		// Будим горутину через рантаймовый семафор, true -> пробуждение в режиме голодании и с handoff.
		// Handoff - передача ownership конкретной горутине воизбежание tail-latency.
		runtime_Semrelease(&m.sema, true, 2)
	}
}
