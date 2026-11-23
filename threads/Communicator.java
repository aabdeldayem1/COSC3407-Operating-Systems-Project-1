package nachos.threads;

import nachos.machine.*;

import java.util.HashMap;

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

    public static void selfTest() {
        System.out.println("\nStarting Task 4 Tests....");
        final Communicator communicator = new Communicator();

        // Test 1: Basic Handoff
        System.out.println("\nTest 1: Basic Handoff");
        final int[] t1Heard = { -1 };

        KThread t1Listener = new KThread(new Runnable() {
            public void run() {
                t1Heard[0] = communicator.listen();
                System.out.println("\tListener heard: " + t1Heard[0]);
            }
        }).setName("T1 Listener");

        KThread t1Speaker = new KThread(new Runnable() {
            public void run() {
                System.out.println("\tSpeaker sending the number 42");
                communicator.speak(42);
            }
        }).setName("T1 Speaker");

        t1Speaker.fork();
        t1Listener.fork();
        t1Speaker.join();
        t1Listener.join();
        Lib.assertTrue(t1Heard[0] == 42, "Test 1 failed: expected 42, got " + t1Heard[0]);
        System.out.println("\tTEST 1 SUCCESSFUL");

        // Test 2: Speaker Arrives First (assert unmatched speaker does not return)
        System.out.println("\nTest 2: Speaker Arrives First");
        final boolean[] t2SpeakerDone = { false };
        final int[] t2Heard = { -1 };

        KThread t2Speaker = new KThread(new Runnable() {
            public void run() {
                System.out.println("\tSpeaker sending 100...");
                communicator.speak(100);
                t2SpeakerDone[0] = true;
            }
        }).setName("T2 Speaker");

        KThread t2Listen = new KThread(new Runnable() {
            public void run() {
                t2Heard[0] = communicator.listen();
                System.out.println("\tListener heard: " + t2Heard[0]);
            }
        }).setName("T2 Listen");

        t2Speaker.fork();
        for (int i = 0; i < 10; i++) {
            KThread.yield();
        }
        Lib.assertTrue(!t2SpeakerDone[0], "Test 2 failed: unmatched speaker returned before listener arrived");

        t2Listen.fork();
        t2Speaker.join();
        t2Listen.join();
        Lib.assertTrue(t2SpeakerDone[0] && t2Heard[0] == 100, "Test 2 failed: expected 100");
        System.out.println("\tTEST 2 SUCCESSFUL");

        // Test 3: Listener Arrives First (assert unmatched listener does not return)
        System.out.println("\nTest 3: Listener Arrives First");
        final boolean[] t3ListenerDone = { false };
        final int[] t3Heard = { -1 };

        KThread t3Listener = new KThread(new Runnable() {
            public void run() {
                t3Heard[0] = communicator.listen();
                t3ListenerDone[0] = true;
                System.out.println("\tListener heard: " + t3Heard[0]);
            }
        }).setName("T3 Listener");

        KThread t3Speaker = new KThread(new Runnable() {
            public void run() {
                System.out.println("\tSpeaker sending 200...");
                communicator.speak(200);
            }
        }).setName("T3 Speaker");

        t3Listener.fork();
        for (int i = 0; i < 10; i++) {
            KThread.yield();
        }
        Lib.assertTrue(!t3ListenerDone[0], "Test 3 failed: unmatched listener returned before speaker arrived");

        t3Speaker.fork();
        t3Speaker.join();
        t3Listener.join();
        Lib.assertTrue(t3ListenerDone[0] && t3Heard[0] == 200, "Test 3 failed: expected 200");
        System.out.println("\tTEST 3 SUCCESSFUL");

        // Test 4: Speaker Queue
        System.out.println("\nTest 4: Speaker Queue");
        KThread[] t4Speakers = new KThread[3];
        final HashMap<Integer, Integer> t4Expected = new java.util.HashMap<Integer, Integer>();
        final HashMap<Integer, Integer> t4Actual = new java.util.HashMap<Integer, Integer>();
        final Lock t4Lock = new Lock();

        for (int i = 0; i < 3; i++) {
            final int val = (i + 1) * 10;
            t4Expected.put(val, 1);
            t4Speakers[i] = new KThread(new Runnable() {
                public void run() {
                    System.out.println("\tQueued Speaker sending " + val + "...");
                    communicator.speak(val);
                    System.out.println("\tQueued Speaker finished " + val);
                }
            }).setName("T4 Speaker " + val);
            t4Speakers[i].fork();
        }

        KThread.yield();

        KThread[] t4Listeners = new KThread[3];
        for (int i = 0; i < 3; i++) {
            final int id = i + 1;
            t4Listeners[i] = new KThread(new Runnable() {
                public void run() {
                    int heard = communicator.listen();
                    System.out.println("\tListener " + id + " heard: " + heard);
                    t4Lock.acquire();
                    Integer cur = t4Actual.get(heard);
                    t4Actual.put(heard, cur == null ? 1 : cur + 1);
                    t4Lock.release();
                }
            }).setName("T4 Listener " + id);
            t4Listeners[i].fork();
        }

        for (int i = 0; i < 3; i++) {
            t4Speakers[i].join();
            t4Listeners[i].join();
        }
        Lib.assertTrue(t4Expected.equals(t4Actual), "Test 4 failed: msg frequencies do not match");
        System.out.println("\tTEST 4 SUCCESSFUL");

        // Test 5: Race cond test with msg freq
        System.out.println("\nTest 5: Race Condition Test");
        final int[] messages = { 5, 10, 15, 20, 20, 25, 30, 35, 40, 45 };
        final int n = messages.length;
        final java.util.HashMap<Integer, Integer> expectedFreq = new java.util.HashMap<Integer, Integer>();
        final java.util.HashMap<Integer, Integer> actualFreq = new java.util.HashMap<Integer, Integer>();
        final Lock freqLock = new Lock();

        for (int i = 0; i < n; i++) {
            Integer cur = expectedFreq.get(messages[i]);
            expectedFreq.put(messages[i], cur == null ? 1 : cur + 1);
        }

        KThread[] raceSpeakers = new KThread[n];
        KThread[] raceListeners = new KThread[n];

        for (int i = 0; i < n; i++) {
            final int val = messages[i];
            raceSpeakers[i] = new KThread(new Runnable() {
                public void run() {
                    communicator.speak(val);
                }
            }).setName("Race Speaker " + i);

            raceListeners[i] = new KThread(new Runnable() {
                public void run() {
                    int heard = communicator.listen();
                    freqLock.acquire();
                    Integer cur = actualFreq.get(heard);
                    actualFreq.put(heard, cur == null ? 1 : cur + 1);
                    freqLock.release();
                }
            }).setName("Race Listener " + i);
        }

        for (int i = 0; i < n; i++) {
            raceSpeakers[i].fork();
            raceListeners[i].fork();
        }

        for (int i = 0; i < n; i++) {
            raceSpeakers[i].join();
            raceListeners[i].join();
        }

        System.out.println("\tExpected freq: " + expectedFreq);
        System.out.println("\tActual freq:   " + actualFreq);
        Lib.assertTrue(expectedFreq.equals(actualFreq), "Test 5 failed: received message freq not matching sent messages");
        System.out.println("\tTEST 5 SUCCESSFUL");
        System.out.println("\n--- Task 4 Tests Completed SUCCESS ---");
    }

}
