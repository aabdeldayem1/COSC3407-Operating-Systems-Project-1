
package nachos.threads;

import nachos.machine.*;

public class ReactWaterTest {

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
            Lib.assertTrue(done[i]);
        }
    }

    public static void selfTest() {

        System.out.println("\nStarting ReactWater Tests....");

        // Test 1: Basic reaction
        System.out.println("\nTest 1: Basic Reaction (2H + 1O)\n");

        ReactWater water1 = new ReactWater();
        boolean[] done1 = new boolean[3];

        KThread[] test1 = {
            hydrogen(water1, done1, 0),
            hydrogen(water1, done1, 1),
            oxygen(water1, done1, 2)
        };

        for (int i = 0; i < test1.length; i++)
            test1[i].fork();

        joinAll(test1);
        checkAllDone(done1);

        System.out.println("\n        TEST 1 SUCCESSFUL");

        // Test 2: Hydrogen waits for Oxygen
        System.out.println("\nTest 2: Hydrogen Threads Wait for Oxygen\n");

        ReactWater water2 = new ReactWater();
        boolean[] done2 = new boolean[3];

        KThread h1 = hydrogen(water2, done2, 0);
        KThread h2 = hydrogen(water2, done2, 1);

        h1.fork();
        KThread.yield();

        h2.fork();
        KThread.yield();

        Lib.assertTrue(!done2[0] && !done2[1]);

        KThread o1 = oxygen(water2, done2, 2);
        o1.fork();

        h1.join();
        h2.join();
        o1.join();

        checkAllDone(done2);

        System.out.println("\n        TEST 2 SUCCESSFUL");

        // Test 3: Oxygen waits for two Hydrogen threads
        System.out.println("\nTest 3: Oxygen Waits for Two Hydrogen Threads\n");

        ReactWater water3 = new ReactWater();
        boolean[] done3 = new boolean[3];

        KThread o2 = oxygen(water3, done3, 0);
        o2.fork();
        KThread.yield();

        Lib.assertTrue(!done3[0]);

        KThread h3 = hydrogen(water3, done3, 1);
        h3.fork();
        KThread.yield();

        KThread h4 = hydrogen(water3, done3, 2);
        h4.fork();

        o2.join();
        h3.join();
        h4.join();

        checkAllDone(done3);

        System.out.println("\n        TEST 3 SUCCESSFUL");

        // Test 4: Excess threads
        System.out.println("\nTest 4: Excess Threads Wait for Another Reaction\n");

        ReactWater water4 = new ReactWater();
        boolean[] done4 = new boolean[6];
        KThread[] test4 = new KThread[6];

        for (int i = 0; i < 4; i++)
            test4[i] = hydrogen(water4, done4, i);

        for (int i = 0; i < 2; i++)
            test4[i + 4] = oxygen(water4, done4, i + 4);

        for (int i = 0; i < test4.length; i++)
            test4[i].fork();

        joinAll(test4);
        checkAllDone(done4);

        System.out.println("\n        TEST 4 SUCCESSFUL");

        // Test 5: Multiple concurrent reactions
        System.out.println("\nTest 5: Multiple Concurrent Reactions\n");

        ReactWater water5 = new ReactWater();
        boolean[] done5 = new boolean[30];
        KThread[] test5 = new KThread[30];

        for (int i = 0; i < 20; i++)
            test5[i] = hydrogen(water5, done5, i);

        for (int i = 0; i < 10; i++)
            test5[i + 20] = oxygen(water5, done5, i + 20);

        for (int i = 0; i < test5.length; i++)
            test5[i].fork();

        joinAll(test5);
        checkAllDone(done5);

        System.out.println("\n        TEST 5 SUCCESSFUL");

        System.out.println("\n--- All ReactWater Tests Completed SUCCESS ---");
    }
}

