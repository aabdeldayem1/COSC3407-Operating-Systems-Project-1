package nachos.threads;
import nachos.machine.*;


public class ReactWater {

    private Lock lock;
    private Condition2 hCond;
    private Condition2 oCond;

    private int waterCount = 0;

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
        waterCount++;
        System.out.println("water was made!");
    }

    private static KThread hydrogen(final ReactWater water,
                                    final boolean[] done,
                                    final int index) {
        return new KThread(new Runnable() {
            public void run() {
                water.hReady();
                done[index] = true;
            }
        }).setName("Hydrogen-" + index);
    }

    private static KThread oxygen(final ReactWater water,
                                  final boolean[] done,
                                  final int index) {
        return new KThread(new Runnable() {
            public void run() {
                water.oReady();
                done[index] = true;
            }
        }).setName("Oxygen-" + index);
    }

    private static void joinAll(KThread[] threads) {
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }

    private static void checkAllDone(boolean[] done) {
        for (int i = 0; i < done.length; i++) {
            Lib.assertTrue(done[i], "Expected thread " + i + " to finish");
        }
    }

    private static int countDone(boolean[] done, int start, int end) {
        int count = 0;
        for (int i = start; i < end; i++) {
            if (done[i]) {
                count++;
            }
        }
        return count;
    }

    public static void selfTest() {
        System.out.println("\nStarting ReactWater Tests....");

        // Test 1: Basic reaction (2H + 1O)
        System.out.println("\nTest 1: Basic Reaction (2H + 1O)");
        ReactWater water1 = new ReactWater();
        boolean[] done1 = new boolean[3];

        KThread[] test1 = {
                hydrogen(water1, done1, 0),
                hydrogen(water1, done1, 1),
                oxygen(water1, done1, 2)
        };

        for (int i = 0; i < test1.length; i++) {
            test1[i].fork();
        }

        joinAll(test1);
        checkAllDone(done1);
        Lib.assertTrue(water1.waterCount == 1, "Test 1: expected 1 molecule, got " + water1.waterCount);
        System.out.println("\tTEST 1 SUCCESSFUL");

        // Test 2: H waits for Oxygen
        System.out.println("\nTest 2: Hydrogen Waits for Oxygen");
        ReactWater water2 = new ReactWater();
        boolean[] done2 = new boolean[3];

        KThread h1 = hydrogen(water2, done2, 0);
        KThread h2 = hydrogen(water2, done2, 1);

        h1.fork();
        KThread.yield();
        h2.fork();
        KThread.yield();

        Lib.assertTrue(!done2[0] && !done2[1], "Test 2: H threads must wait for O");
        Lib.assertTrue(water2.waterCount == 0, "Test 2: no molecule should form before Oxygen arrives");

        KThread o1 = oxygen(water2, done2, 2);
        o1.fork();

        h1.join();
        h2.join();
        o1.join();

        checkAllDone(done2);
        Lib.assertTrue(water2.waterCount == 1, "Test 2: expected 1 molecule, got " + water2.waterCount);
        System.out.println("\tTEST 2 SUCCESSFUL");

        // Test 3
        System.out.println("\nTest 3: Oxygen Waits for Two Hydrogen Threads");
        ReactWater water3 = new ReactWater();
        boolean[] done3 = new boolean[3];

        KThread o2 = oxygen(water3, done3, 0);
        o2.fork();
        KThread.yield();

        Lib.assertTrue(!done3[0], "Test 3: Oxygen thread must wait for H");
        Lib.assertTrue(water3.waterCount == 0, "Test 3: no molecule should form yet");

        KThread h3 = hydrogen(water3, done3, 1);
        h3.fork();
        KThread.yield();

        Lib.assertTrue(!done3[0] && !done3[1], "Test 3: Oxygen and 1st H must wait for 2nd H");
        Lib.assertTrue(water3.waterCount == 0, "Test 3: no molecule should form with only 1H + 1O");

        KThread h4 = hydrogen(water3, done3, 2);
        h4.fork();

        o2.join();
        h3.join();
        h4.join();

        checkAllDone(done3);
        Lib.assertTrue(water3.waterCount == 1, "Test 3: expected 1 molecule, got " + water3.waterCount);
        System.out.println("\tTEST 3 SUCCESSFUL");

        // Test 4
        System.out.println("\nTest 4: Balanced Multiple Reactions (4H + 2O)");
        ReactWater water4 = new ReactWater();
        boolean[] done4 = new boolean[6];
        KThread[] test4 = new KThread[6];

        for (int i = 0; i < 4; i++) {
            test4[i] = hydrogen(water4, done4, i);
        }
        for (int i = 0; i < 2; i++) {
            test4[i + 4] = oxygen(water4, done4, i + 4);
        }

        for (int i = 0; i < test4.length; i++) {
            test4[i].fork();
        }

        joinAll(test4);
        checkAllDone(done4);
        Lib.assertTrue(water4.waterCount == 2, "Test 4: expected 2 molecules, got " + water4.waterCount);
        System.out.println("\tTEST 4 SUCCESSFUL");

        // Test 5: Multiple rxns
        System.out.println("\nTest 5: Multiple reactions 20H + 10 oxygens");
        ReactWater water5 = new ReactWater();
        boolean[] done5 = new boolean[30];
        KThread[] test5 = new KThread[30];

        for (int i = 0; i < 20; i++) {
            test5[i] = hydrogen(water5, done5, i);
        }
        for (int i = 0; i < 10; i++) {
            test5[i + 20] = oxygen(water5, done5, i + 20);
        }

        for (int i = 0; i < test5.length; i++) {
            test5[i].fork();
        }

        joinAll(test5);
        checkAllDone(done5);
        Lib.assertTrue(water5.waterCount == 10, "Test 5: expected 10 molecules, got " + water5.waterCount);
        System.out.println("\tTEST 5 SUCCESSFUL");

        // Test 6: Shortage (1H + 1O -> 0 molecules, both remain blocked)
        System.out.println("\nTest 6: Shortage 1H + 1O");
        ReactWater water6 = new ReactWater();
        boolean[] done6 = new boolean[2];

        hydrogen(water6, done6, 0).fork();
        oxygen(water6, done6, 1).fork();

        for (int i = 0; i < 20; i++) {
            KThread.yield();
        }

        Lib.assertTrue(water6.waterCount == 0, "Test 6: expected 0 molecules, got " + water6.waterCount);
        Lib.assertTrue(countDone(done6, 0, 1) == 0, "Test 6: unmatched H must remain blocked");
        Lib.assertTrue(countDone(done6, 1, 2) == 0, "Test 6: unmatched O must remain blocked");
        System.out.println("\tTEST 6 SUCCESSFUL");

        // Test 7
        System.out.println("\nTest 7: Surplus Test (4H + 3O)");
        ReactWater water7 = new ReactWater();
        boolean[] done7 = new boolean[7];

        for (int i = 0; i < 4; i++) {
            hydrogen(water7, done7, i).fork();
        }
        for (int i = 0; i < 3; i++) {
            oxygen(water7, done7, i + 4).fork();
        }

        for (int i = 0; i < 50; i++) {
            KThread.yield();
        }

        int hReturned = countDone(done7, 0, 4);
        int oReturned = countDone(done7, 4, 7);
        Lib.assertTrue(water7.waterCount == 2, "Test 7: expected 2 molecules, got " + water7.waterCount);
        Lib.assertTrue(hReturned == 4, "Test 7: expected 4 H returned, got " + hReturned);
        Lib.assertTrue(oReturned == 2, "Test 7: expected 2 O returned (1 surplus blocked), got " + oReturned);
        System.out.println("\tTEST 7 SUCCESSFUL");

        System.out.println("\n--- All ReactWater Tests Completed SUCCESS ---");
    }
}
