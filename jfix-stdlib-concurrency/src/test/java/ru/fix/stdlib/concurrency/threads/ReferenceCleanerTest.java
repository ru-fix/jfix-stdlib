package ru.fix.stdlib.concurrency.threads;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReferenceCleanerTest {

    @Test
    void usage() throws Exception {
        AtomicBoolean disposedFlag = new AtomicBoolean(false);

        System.out.println("Create 100KB weakly reachable objects and register in cleaner");
        ArrayList<Object> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int[] obj = new int[1024];
            Arrays.fill(obj, i);
            data.add(obj);
            ReferenceCleaner.register(obj, i, meta -> disposedFlag.set(true));
        }
        data.clear();

        System.out.println("Create garbage and wait for cleaning event");

        long start = System.currentTimeMillis();
        while (!disposedFlag.get() || System.currentTimeMillis() - start > Duration.of(2, ChronoUnit.SECONDS).toMillis()) {
            System.out.println("Running time: " + Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
            System.out.println("Occupied memory: " + (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1024 + " Kb");

            for (int mb = 0; mb < 10; mb++) {
                for (int kb = 0; kb < 1024; kb++) {
                    int[] obj = new int[1024];
                    data.add(obj);
                }
            }
            data.clear();
            Thread.sleep(1000);
        }
        assertTrue(disposedFlag.get());
    }
}
