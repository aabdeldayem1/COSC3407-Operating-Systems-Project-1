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
}
