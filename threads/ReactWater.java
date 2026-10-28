package nachos.threads;
import nachos.machine.*;


public class ReactWater {

    private Lock lock;
    private Condition2 hCond;
    private Condition2 oCond;

    private int hWaiting;
    private int oWaiting;

    public ReactWater() {
        lock = new Lock();

        hCond = new Condition2(lock);
        oCond = new Condition2(lock);

        hWaiting = 0;
        oWaiting = 0;
    }

    /**
     *   When H element comes, if there already exist another H element
     *   and an O element, then call the method of Makewater(). Or let
     *   H element wait in line.
     **/
    public void hReady(){
        lock.acquire();
        hWaiting++;

        if (hWaiting >= 2 && oWaiting >= 1){
            hWaiting -= 2;
            oWaiting -= 1;

            makeWater();

            hCond.wake();
            oCond.wake();
        }
        else hCond.sleep();

        lock.release();
    }

    /**
     *   When O element comes, if there already exist another two H
     *   elements, then call the method of Makewater(). Or let O element
     *   wait in line.
     **/
    public void oReady(){
        lock.acquire();
        oWaiting++;

        if (oWaiting >= 1 && hWaiting >= 2){
            hWaiting -= 2;
            oWaiting -= 1;

            makeWater();

            hCond.wake();
            hCond.wake();

        }
        else oCond.sleep();

        lock.release();
    }

    /**
     *   Print out the message of "water was made!".
     **/
    private void makeWater(){
        System.out.println("water was made!");
    }
}
