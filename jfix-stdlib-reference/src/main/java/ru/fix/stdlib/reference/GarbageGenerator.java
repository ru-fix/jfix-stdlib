package ru.fix.stdlib.reference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.function.Supplier;

public class GarbageGenerator {
    private static final Logger log = LoggerFactory.getLogger(GarbageGenerator.class);

    private Duration timeout = Duration.ofMinutes(1);
    private Duration delay = Duration.ofMillis(100);
    private int garbageSizePerIterationMB = 10;

    public GarbageGenerator setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public GarbageGenerator setDelay(Duration delay) {
        this.delay = delay;
        return this;
    }

    public GarbageGenerator setGarbageSizePerIterationMB(int garbageSizePerIterationMB) {
        this.garbageSizePerIterationMB = garbageSizePerIterationMB;
        return this;
    }

    boolean generateGarbageAndWaitForCondition(Supplier<Boolean> condition) throws Exception {
        ArrayList<Object> data = new ArrayList<>();
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start <= timeout.toMillis()) {
            if (condition.get()) {
                return true;
            }

            log.info("" +
                    "Running time: " +
                    Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS) + "\n" +
                    "Occupied memory: " +
                    (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 + " Kb");

            for (int mb = 0; mb < garbageSizePerIterationMB; mb++) {
                for (int kb = 0; kb < 1024; kb++) {
                    byte[] obj = new byte[1024];
                    data.add(obj);
                }
            }
            data.clear();
            Thread.sleep(delay.toMillis());
        }
        return condition.get();
    }
}
