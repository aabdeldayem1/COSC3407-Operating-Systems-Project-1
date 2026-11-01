package nachos.threads;

import nachos.machine.*;

import java.util.PriorityQueue;

/**
 * Uses the hardware timer to provide preemption, and to allow threads to sleep
 * until a certain time.
 */
public class Alarm {

    private PriorityQueue<WaitingThread> waitQueue;
    /**
     * Allocate a new Alarm. Set the machine's timer interrupt handler to this
     * alarm's callback.
     *
     * <p><b>Note</b>: Nachos will not function correctly with more than one
     * alarm.
     */
    public Alarm() {
	    Machine.timer().setInterruptHandler(new Runnable() {
		    public void run() { timerInterrupt(); }
	        });

        waitQueue = new PriorityQueue<WaitingThread>();
    }

    /**
     * The timer interrupt handler. This is called by the machine's timer
     * periodically (approximately every 500 clock ticks). Causes the current
     * thread to yield, forcing a context switch if there is another thread
     * that should be run.
     */
    public void timerInterrupt(){
        boolean intStatus = Machine.interrupt().disable();

        long currentTime = Machine.timer().getTime();

        while(!waitQueue.isEmpty() && (waitQueue.peek().wakeTime <= currentTime)){
            WaitingThread thread = waitQueue.poll();
            if(thread != null){
                thread.thread.ready();
            }
        }

        Machine.interrupt().restore(intStatus);

        KThread.currentThread().yield();
    }

    /**
     * Put the current thread to sleep for at least <i>x</i> ticks,
     * waking it up in the timer interrupt handler. The thread must be
     * woken up (placed in the scheduler ready set) during the first timer
     * interrupt where
     *
     * <p><blockquote>
     * (current time) >= (WaitUntil called time)+(x)
     * </blockquote>
     *
     * @param	x	the minimum number of clock ticks to wait.
     *
     * @see	nachos.machine.Timer#getTime()
     */
    public void waitUntil(long x) {
	    if(x <= 0){
            return;
        }

        long wakeTime = Machine.timer().getTime() + x;

        boolean intStatus = Machine.interrupt().disable();

        waitQueue.add(new WaitingThread(KThread.currentThread(), wakeTime));
        KThread.sleep();

        Machine.interrupt().restore(intStatus);
    }


    private class WaitingThread implements Comparable<WaitingThread> {
        KThread thread;
        long wakeTime;

        public WaitingThread(KThread thread, long wakeTime) {
            this.thread = thread;
            this.wakeTime = wakeTime;
        }

        @Override
        public int compareTo(WaitingThread other) {
            if (this.wakeTime < other.wakeTime) {
                return -1;
            } else if (this.wakeTime > other.wakeTime) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * Tests whether this module is working.
     */
    public static void selfTest() {
		System.out.println("Starting Task 3 Tests....");

		//test 1
		System.out.println("\nTest 1: Basic Wait");
		final long[] elapsed = new long[1];
		final int[] otherRuns = new int[1];
		final boolean[] sleeperDone = new boolean[1];

		KThread sleeper = new KThread(new Runnable() {
			public void run() {
				long before = Machine.timer().getTime();
				System.out.println("\tSleeper calling waitUntil(500) at time " + before);
				ThreadedKernel.alarm.waitUntil(500);
				long after = Machine.timer().getTime();
				elapsed[0] = after - before;
				sleeperDone[0] = true;
				System.out.println("\tSleeper woke at time " + after + " (elapsed " + elapsed[0] + ")");
			}
		}).setName("Sleeper");

		//if the sleeper busy-waited, this thread would barely get to run
		KThread other = new KThread(new Runnable() {
			public void run() {
				while (!sleeperDone[0]) {
					otherRuns[0]++;
					KThread.yield();
				}
			}
		}).setName("Other");

		sleeper.fork();
		other.fork();
		sleeper.join();
		other.join();

		Lib.assertTrue(elapsed[0] >= 500, "Test 1 failed: slept only " + elapsed[0] + " ticks");
		Lib.assertTrue(otherRuns[0] > 0, "Test 1 failed: other thread never ran while sleeping");
		System.out.println("\tSuccess: slept >= 500 ticks and other thread ran " + otherRuns[0] + " times");


		//test 2
		System.out.println("\n\n Test 2: Negative wait");

		final long[] negElapsed = new long[1];
		final boolean[] negFinished = new boolean[1];

		KThread negThread = new KThread(new Runnable() {
			public void run() {
				long before = Machine.timer().getTime();
				System.out.println("\tThread calling waitUntil(-10)...");
				ThreadedKernel.alarm.waitUntil(-10);
				negElapsed[0] = Machine.timer().getTime() - before;
				negFinished[0] = true;
				System.out.println("\tThread returned immediately (elapsed " + negElapsed[0] + ")");
			}
		}).setName("Negative thread");

		negThread.fork();
		negThread.join();

		Lib.assertTrue(negFinished[0], "Test 2 failed: thread never finished");
		Lib.assertTrue(negElapsed[0] < 500, "Test 2 failed: thread appears to have slept");
		System.out.println("\tSuccess: no sleep and no error");


		//test 3
		System.out.println("\n\n Test 3: Multiple threads, different times");

		final long[] finishA = new long[1];
		final long[] finishB = new long[1];

		KThread threadA = new KThread(new Runnable() {
			public void run() {
				System.out.println("\tThread A calling waitUntil(2000)");
				ThreadedKernel.alarm.waitUntil(2000);
				finishA[0] = Machine.timer().getTime();
				System.out.println("\tThread A woke at time " + finishA[0]);
			}
		}).setName("Thread A");

		KThread threadB = new KThread(new Runnable() {
			public void run() {
				System.out.println("\tThread B calling waitUntil(100)");
				ThreadedKernel.alarm.waitUntil(100);
				finishB[0] = Machine.timer().getTime();
				System.out.println("\tThread B woke at time " + finishB[0]);
			}
		}).setName("Thread B");

		threadA.fork();
		threadB.fork();
		threadA.join();
		threadB.join();

		Lib.assertTrue(finishB[0] < finishA[0], "Test 3 failed: B did not wake before A");
		Lib.assertTrue(finishB[0] >= 100 && finishA[0] >= 2000, "Test 3 failed: a thread woke too early");
		System.out.println("\tSuccess: B woke before A");


		//test 4
		System.out.println("\n\n Test 4: Multiple threads, same time");

		final int numThreads = 5;
		final int[] wokeCount = new int[1];
		final long[] sameElapsed = new long[numThreads];
		KThread[] threads = new KThread[numThreads];

		for (int i = 0; i < numThreads; i++) {
			final int id = i;
			threads[i] = new KThread(new Runnable() {
				public void run() {
					long before = Machine.timer().getTime();
					ThreadedKernel.alarm.waitUntil(200);
					sameElapsed[id] = Machine.timer().getTime() - before;
					wokeCount[0]++;
					System.out.println("\tThread " + id + " woke (elapsed " + sameElapsed[id] + ")");
				}
			}).setName("Same-time thread " + i);
		}

		for (int i = 0; i < numThreads; i++) {
			threads[i].fork();
		}

		//join hangs here if a thread was lost
		for (int i = 0; i < numThreads; i++) {
			threads[i].join();
		}

		Lib.assertTrue(wokeCount[0] == numThreads, "Test 4 failed: only " + wokeCount[0] + " of " + numThreads + " woke");
		for (int i = 0; i < numThreads; i++) {
			Lib.assertTrue(sameElapsed[i] >= 200, "Test 4 failed: thread " + i + " slept only " + sameElapsed[i]);
		}
		System.out.println("\tSuccess: all " + numThreads + " threads woke");


		System.out.println("\n--- Task 3 Tests Completed SUCCESS ---");
    }
}
