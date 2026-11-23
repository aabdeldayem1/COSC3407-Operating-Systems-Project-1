package nachos.threads;

import nachos.machine.*;

/**
 * An implementation of condition variables that disables interrupt()s for
 * synchronization.
 *
 * <p>
 * You must implement this.
 *
 * @see	nachos.threads.Condition
 */
public class Condition2 {

    private Lock conditionLock;
    private ThreadQueue waitQueue;

    /**
     * Allocate a new condition variable.
     *
     * @param	conditionLock	the lock associated with this condition
     *				variable. The current thread must hold this
     *				lock whenever it uses <tt>sleep()</tt>,
     *				<tt>wake()</tt>, or <tt>wakeAll()</tt>.
     */
    public Condition2(Lock conditionLock) {
	    this.conditionLock = conditionLock;
        waitQueue = ThreadedKernel.scheduler.newThreadQueue(false);
    }

    /**
     * Atomically release the associated lock and go to sleep on this condition
     * variable until another thread wakes it using <tt>wake()</tt>. The
     * current thread must hold the associated lock. The thread will
     * automatically reacquire the lock before <tt>sleep()</tt> returns.
     */
    public void sleep() {
	    Lib.assertTrue(conditionLock.isHeldByCurrentThread());

        boolean intStatus = Machine.interrupt().disable();

        waitQueue.waitForAccess(KThread.currentThread());

        conditionLock.release();

        KThread.sleep();

        Machine.interrupt().restore(intStatus);

	    conditionLock.acquire();
    }

    /**
     * Wake up at most one thread sleeping on this condition variable. The
     * current thread must hold the associated lock.
     */
    public void wake() {
	    Lib.assertTrue(conditionLock.isHeldByCurrentThread());

        boolean intStatus = Machine.interrupt().disable();

        KThread nextThread = waitQueue.nextThread();

        if(nextThread != null) {
            nextThread.ready();
        }

        Machine.interrupt().restore(intStatus);
    }

    /**
     * Wake up all threads sleeping on this condition variable. The current
     * thread must hold the associated lock.
     */
    public void wakeAll() {
	    Lib.assertTrue(conditionLock.isHeldByCurrentThread());

        boolean intStatus = Machine.interrupt().disable();

        KThread nextThread = waitQueue.nextThread();

        while(nextThread != null) {
            nextThread.ready();
            nextThread = waitQueue.nextThread();
        }

        Machine.interrupt().restore(intStatus);
    }

    public static void selfTest(){
        System.out.println("\n\n\n Stating Task 2 Tests....");

        Lock lock = new Lock();
        Condition2 cond = new Condition2(lock);

        //test 1
        System.out.println("\n Test 1: Sleep and wake");
        final boolean[] ready1 = { false };
        final int[] woke1 = { 0 };
        KThread sleeper1 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\t Thread A has lock. sleeping");
                ready1[0] = true;
                cond.sleep();
                woke1[0]++;
                System.out.println("\t Thread A is awake, reaquired lock, and finishing tasks");
                lock.release();
            }
        }).setName("Thread A");

        sleeper1.fork();

        while (true) {
            lock.acquire();
            if (ready1[0]) {
                System.out.println("\tMain thread calling wake()");
                cond.wake();
                lock.release();
                break;
            }
            lock.release();
            KThread.yield();
        }

        sleeper1.join();
        Lib.assertTrue(woke1[0] == 1, "Test 1: Expected exactly 1 thread to resume");
        System.out.println("\n\tTEST 1 SUCCESSFUL");

        //test 2

        System.out.println("\nTest 2: Multiple waiting threads...");
        final int[] ready2 = { 0 };
        final int[] woke2 = { 0 };
        KThread sleeper2 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread B acquired lock, calling sleep()...");
                ready2[0]++;
                cond.sleep();
                woke2[0]++;
                System.out.println("\tThread B woke up and finishing.");
                lock.release();
            }
        }).setName("Thread B");

        KThread sleeper3 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread C acquired lock, calling sleep()...");
                ready2[0]++;
                cond.sleep();
                woke2[0]++;
                System.out.println("\tThread C woke up and finishing.");
                lock.release();
            }
        }).setName("Thread C");

        sleeper2.fork();
        sleeper3.fork();
        // Handshake until both B and C are sleeping
        while (true) {
            lock.acquire();
            if (ready2[0] == 2) {
                System.out.println("\tMain thread calling wake() ONCE (should wake exactly 1)...");
                cond.wake();
                lock.release();
                break;
            }
            lock.release();
            KThread.yield();
        }

        // Wait until the single woken thread wakes up
        while (true) {
            lock.acquire();
            if (woke2[0] == 1) {
                lock.release();
                break;
            }
            lock.release();
            KThread.yield();
        }
        KThread.yield();

        lock.acquire();
        Lib.assertTrue(woke2[0] == 1, "Test 2: wake() must resume exactly 1 thread, got " + woke2[0]);
        System.out.println("\tMain thread calling wake() a second time...");
        cond.wake();
        lock.release();

        sleeper2.join();
        sleeper3.join();
        Lib.assertTrue(woke2[0] == 2, "Test 2: Expected 2 total threads resumed, got " + woke2[0]);
        System.out.println("\n\tTEST 2 SUCCESSFUL");

        //test 3
        System.out.println("\n Test 3: Wake all threads");
        final int[] ready3 = { 0 };
        final int[] woke3 = { 0 };
        KThread sleeper4 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread D acquired lock, calling sleep()...");
                ready3[0]++;
                cond.sleep();
                woke3[0]++;
                System.out.println("\tThread D woke up and finishing.");
                lock.release();
            }
        }).setName("Thread D");
        KThread sleeper5 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread E acquired lock, calling sleep()...");
                ready3[0]++;
                cond.sleep();
                woke3[0]++;
                System.out.println("\tThread E woke up and finishing.");
                lock.release();
            }
        }).setName("Thread E");

        sleeper4.fork();
        sleeper5.fork();
        while (true) {
            lock.acquire();
            if (ready3[0] == 2) {
                System.out.println("\tMain thread calling wakeAll()...");
                cond.wakeAll();
                lock.release();
                break;
            }
            lock.release();
            KThread.yield();
        }

        sleeper4.join();
        sleeper5.join();
        Lib.assertTrue(woke3[0] == 2, "Test 3: wakeAll() must resume both threads, got " + woke3[0]);
        System.out.println("\n\tTEST 3 SUCCESSFUL");

        //test 4

        System.out.println("\nTest 4: Empty waiting list");
        System.out.println("\tMain thread calling wake() and wakeAll() with 0 sleeping threads...");
        lock.acquire();
        cond.wake();
        cond.wakeAll();
        lock.release();

        final boolean[] ready4 = { false };
        final boolean[] woke4 = { false };
        KThread lateSleeper = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                ready4[0] = true;
                cond.sleep();
                woke4[0] = true;
                lock.release();
            }
        }).setName("Late Sleeper");
        lateSleeper.fork();

        // Handshake until lateSleeper is asleep; verify it did NOT immediately wake
        while (true) {
            lock.acquire();
            if (ready4[0]) {
                Lib.assertTrue(!woke4[0], "Test 4: Empty wake must not be saved for a later sleeper");
                cond.wake();
                lock.release();
                break;
            }
            lock.release();
            KThread.yield();
        }
        lateSleeper.join();
        Lib.assertTrue(woke4[0], "Test 4: Late sleeper should wake on subsequent wake()");
        System.out.println("\n\tTEST 4 SUCCESSFUL");

        // Test 5: Calling sleep/wake/wakeAll without holding the lock
        System.out.println("\nTest 5: Calls without holding the lock");
        boolean prevInt = Machine.interrupt().enabled();
        boolean caughtSleep = false;
        boolean caughtWake = false;
        boolean caughtWakeAll = false;

        try {
            cond.sleep();
        } catch (Error e) {
            caughtSleep = true;
        } finally {
            Machine.interrupt().restore(prevInt);
        }

        try {
            cond.wake();
        } catch (Error e) {
            caughtWake = true;
        } finally {
            Machine.interrupt().restore(prevInt);
        }

        try { cond.wakeAll();
        } catch (Error e) {
            caughtWakeAll = true;
        } finally {
            Machine.interrupt().restore(prevInt);
        }

        Lib.assertTrue(caughtSleep, "Test 5: sleep() without lock must assert");
        Lib.assertTrue(caughtWake, "Test 5: wake() without lock must assert");
        Lib.assertTrue(caughtWakeAll, "Test 5: wakeAll() without lock must assert");
        Lib.assertTrue(Machine.interrupt().enabled() == prevInt, "Test 5: Interrupt state not restored");
        System.out.println("\n\tTEST 5 SUCCESSFUL");

        System.out.println("\n--- Task 2 Tests Completed SUCCESS ---");
    }


}
