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
        KThread sleeper1 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\t Thread A has lock. sleeping");
                cond.sleep();
                System.out.println("\t Thread A is awake, reaquired lock, and finishing tasks");
                lock.release();
            }
        }).setName("Thraed A");

        sleeper1.fork();
        KThread.yield();

        System.out.println("\t Main thread aquiring lock to call wake()");
        lock.acquire();
        cond.wake();
        System.out.println("\t Main thread called wake() and released lock");
        lock.release();
        sleeper1.join();

        System.out.println("\n\tTEST 1 SUCCESSFUL");

        //test 2

        System.out.println("\nTest 2: Multiple waiting threads...");
        KThread sleeper2 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread B acquired lock, calling sleep()...");
                cond.sleep();
                System.out.println("\tThread B woke up and finishing.");
                lock.release();
            }
        }).setName("Thread B");

        KThread sleeper3 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread C acquired lock, calling sleep()...");
                cond.sleep();
                System.out.println("\tThread C woke up and finishing.");
                lock.release();
            }
        }).setName("Thread C");

        sleeper2.fork();
        sleeper3.fork();
        KThread.yield();
        KThread.yield();

        System.out.println("\tMain thread calling wake() ONCE (should only wake Thread B)...");
        lock.acquire();
        cond.wake();
        lock.release();

        KThread.yield();

        System.out.println("\tremoving Thread C to proceed...");
        lock.acquire();
        cond.wake();
        lock.release();

        sleeper2.join();
        sleeper3.join();

        System.out.println("\n\tTEST 2 SUCCESSFUL");

        //test 3
        System.out.println("\n Test 3: Wake all threads");
        KThread sleeper4 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread D acquired lock, calling sleep()...");
                cond.sleep();
                System.out.println("\tThread D woke up and finishing.");
                lock.release();
            }
        }).setName("Thread D");
        KThread sleeper5 = new KThread(new Runnable() {
            public void run() {
                lock.acquire();
                System.out.println("\tThread E acquired lock, calling sleep()...");
                cond.sleep();
                System.out.println("\tThread E woke up and finishing.");
                lock.release();
            }
        }).setName("Thread E");

        sleeper4.fork();
        sleeper5.fork();
        KThread.yield();
        KThread.yield();

        System.out.println("\tmain thread calling wakeAll() .... ");
        lock.acquire();
        cond.wakeAll();
        lock.release();

        sleeper4.join();
        sleeper5.join();

        System.out.println("\n\tTEST 3 SUCCESSFUL");

        //test 4

        System.out.println("\nTest 4: Empty waiting list");
        System.out.println("\tMain thread calling wake() and wakeAll() with 0 sleeping threads...");
        lock.acquire();
        cond.wake();
        cond.wakeAll();
        lock.release();
        System.out.println("\tMain thread successfully returned from wake() and wakeAll() without errors.");

        System.out.println("\n--- Task 2 Tests Completed SUCCESS ---");
    }


}
