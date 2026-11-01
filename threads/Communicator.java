package nachos.threads;

import nachos.machine.*;

/**
 * A <i>communicator</i> allows threads to synchronously exchange 32-bit
 * messages. Multiple threads can be waiting to <i>speak</i>,
 * and multiple threads can be waiting to <i>listen</i>. But there should never
 * be a time when both a speaker and a listener are waiting, because the two
 * threads can be paired off at this point.
 */
public class Communicator {
    /**
     * Allocate a new communicator.
     */

    private Lock lock;
    private Condition2 waitToSpeak;
    private Condition2 waitToListen;
    private Condition2 waitToAck;

    private boolean activSpeaker;
    private boolean hasMessage;
    private int currentWord;


    public Communicator() {
        lock = new Lock();
        waitToSpeak = new Condition2(lock);
        waitToListen = new Condition2(lock);
        waitToAck = new Condition2(lock);

        activSpeaker = false;
        hasMessage = false;
        currentWord = 0;
    }

    /**
     * Wait for a thread to listen through this communicator, and then transfer
     * <i>word</i> to the listener.
     *
     * <p>
     * Does not return until this thread is paired up with a listening thread.
     * Exactly one listener should receive <i>word</i>.
     *
     * @param	word	the integer to transfer.
     */
    public void speak(int word) {
        lock.acquire();

        while (activSpeaker) {
            waitToSpeak.sleep();
        }

        activSpeaker = true;
        currentWord = word;
        hasMessage = true;

        waitToListen.wake();

        while (hasMessage) {
            waitToAck.sleep();
        }

        activSpeaker = false;
        waitToSpeak.wake();

        lock.release();
    }

    /**
     * Wait for a thread to speak through this communicator, and then return
     * the <i>word</i> that thread passed to <tt>speak()</tt>.
     *
     * @return	the integer transferred.
     */    
    public int listen() {
	    lock.acquire();

        while(!hasMessage) {
            waitToListen.sleep();
        }

        int saveWord = currentWord;
        hasMessage = false;

        waitToAck.wake();

        lock.release();
        return saveWord;
    }

    public static void selfTest(){
        System.out.println("\nStarting Task 4 Tests....");
        Communicator communicator = new Communicator();
        //test 1
        System.out.println("\nTest 1: Basic Handoff");

        KThread t1Listener = new KThread(() -> {
            System.out.println("\tListener heard: " + communicator.listen());
        }).setName("T1 Listener");

        KThread t1Speaker = new KThread(() -> {
            System.out.println("\tSpeaker sending the number 42");
            communicator.speak(42);
        }).setName("T1 Speaker");

        t1Speaker.fork();
        t1Listener.fork();
        t1Speaker.join();
        t1Listener.join();

        System.out.println("\tTEST 1 SUCCESSFUL");

        System.out.println("\nTest 2: Speaker Arrives First");
        KThread t2Speaker = new KThread(() -> {
            System.out.println("\tSpeaker sending 100...");
            communicator.speak(100);
        }).setName("T2 Speaker");

        KThread t2Listen = new KThread(() -> {
            System.out.println("\tListener heard: " + communicator.listen());
        }).setName("T2 Listen");

        t2Speaker.fork();
        KThread.yield();
        KThread.yield();
        t2Listen.fork();

        t2Speaker.join();
        t2Listen.join();
        System.out.println("\tTEST 2 SUCCESSFUL");

        System.out.println("\nTest 3: Listener Arrives first");

        KThread t3Listener = new KThread(() -> {
            System.out.println("\tListener heard: " + communicator.listen());
        }).setName("T3 Listener");

        KThread t3Speaker = new KThread(() -> {
            System.out.println("\tSpeaker sending 200...");
            communicator.speak(200);
        }).setName("T3 Speaker");

        t3Listener.fork();
        KThread.yield();
        KThread.yield();
        t3Speaker.fork();

        t3Speaker.join();
        t3Listener.join();
        System.out.println("\tTEST 3 SUCCESSFUL");

        System.out.println("\nTest 4: Speaker Queue");
        KThread[] t4Speakers = new KThread[3];

        for(int i =0 ; i < 3; i++){
            final int val = i + 1;
            t4Speakers[i] = new KThread(() -> {
                System.out.println("\tQueued Speaker " + val + " sending " + (val * 10) +"....");
                communicator.speak(val*10);
                System.out.println("\tQueued Speaker " + val + " finished");
            }).setName("T4 Speaker" + val);
            t4Speakers[i].fork();
        }

        KThread.yield();

        KThread t4Listener1 = new KThread(() -> {
            System.out.println("\tListener 1 heard: " + communicator.listen());
        }).setName("T4 Listener");

        t4Listener1.fork();
        System.out.println("\tCreating 2 more listeners to remove the queued threads");
        KThread t4Listener2 = new KThread(() -> {
            System.out.println("\tListener 2 heard: " + communicator.listen());
        });
        KThread t4Listener3 = new KThread(() -> {
            System.out.println("\tListener 3 heard: " + communicator.listen());
        });

        t4Listener2.fork();
        t4Listener3.fork();

        for(int i = 0; i < 3; i++) t4Speakers[i].join();

        t4Listener1.join();
        t4Listener2.join();
        t4Listener3.join();
        System.out.println("\tTEST 4 SUCCESSFUL");

        System.out.println("\nTest 5: Race Condition Test");
        final int[] sums = {0, 0};
        Lock sumLock = new Lock();
        KThread[] raceSpeakers = new KThread[10];
        KThread[] raceListeners = new KThread[10];

        for(int i =0 ; i < 10; i++){
            final int val = (i + 1) * 5;
            raceSpeakers[i] = new KThread(() -> {
                communicator.speak(val);
                sumLock.acquire();
                sums[0] += val;
                sumLock.release();
            }).setName("Race Speaker" + val);

            raceListeners[i] = new KThread(() -> {
                int heard = communicator.listen();
                sumLock.acquire();
                sums[1] += heard;
                sumLock.release();
            }).setName("Race Listener" + val);
        }

        for(int i =0 ; i < 10; i++){
            raceSpeakers[i].fork();
            raceListeners[i].fork();
        }

        for(int i =0 ; i < 10; i++){
            raceSpeakers[i].join();
            raceListeners[i].join();
        }

        System.out.println("\tTotal Sent Sum: " + sums[0]);
        System.out.println("\tTotal received Sum: " + sums[1]);

        if(sums[0] == sums[1] && sums[0] > 0){
            System.out.println("\tTest 5 SUCCESSFUL");
            System.out.println("\n--- Task 4 Tests Completed SUCCESS ---");
        } else {
            System.out.println("\tTest 5 FAILED");
            System.out.println("\n--- Task 4 Tests FAILED ---");
        }
    }

}
