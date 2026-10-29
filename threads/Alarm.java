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
}
