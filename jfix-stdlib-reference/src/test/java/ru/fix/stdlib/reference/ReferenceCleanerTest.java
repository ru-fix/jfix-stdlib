package ru.fix.stdlib.reference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;


public class ReferenceCleanerTest {

    static final Duration GENERATE_GARBAGE_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);

    static final ReferenceCleaner referenceCleaner = ReferenceCleaner.getInstance();

    @Test
    void two_sequential_cleans_leads_to_thread_restart() throws Exception {
        AtomicInteger disposedObjects = new AtomicInteger(0);

        Object myObject1 = new Object();
        referenceCleaner.register(myObject1, null, (ref, meta) -> disposedObjects.incrementAndGet());
        myObject1 = null;

        assertTrue(
                generateGarbageAndWaitForCondition(GENERATE_GARBAGE_TIMEOUT, () -> disposedObjects.get() == 1),
                () -> "disposed objects: " + disposedObjects.get()
        );


        Object myObject2 = new Object();
        referenceCleaner.register(myObject2, null, (ref, meta) -> disposedObjects.incrementAndGet());
        myObject2 = null;

        assertTrue(
                generateGarbageAndWaitForCondition(GENERATE_GARBAGE_TIMEOUT, () -> disposedObjects.get() == 2),
                () -> "disposed objects: " + disposedObjects.get()
        );
    }

    @CsvSource({
            "1, false",
            "5, false",
            "1, true",
            "15, true",
    })
    @ParameterizedTest
    void cleaning_action_invoked_when_object_became_unreachable(int countOfObjects, boolean keepWeakReference) throws Exception {
        AtomicInteger disposedObjects = new AtomicInteger(0);
        ArrayList<CleanableWeakReference> refs = new ArrayList<>();

        for (int i = 0; i < countOfObjects; i++) {
            Object myObject = new Object();
            CleanableWeakReference<Object> ref = referenceCleaner.register(myObject, null, (r, m) -> disposedObjects.incrementAndGet());
            if(keepWeakReference){
                refs.add(ref);
            }
            myObject = null;
        }

        System.out.println("Create garbage and wait for " + countOfObjects + " cleaning events");
        assertTrue(
                generateGarbageAndWaitForCondition(GENERATE_GARBAGE_TIMEOUT, () -> disposedObjects.get() == countOfObjects),
                () -> "disposed objects: " + disposedObjects.get()
        );
    }


    @Test
    void access_object_scheduled_for_cleaning() throws Exception {
        Object myObject = new Object();
        CleanableWeakReference<Object> ref = referenceCleaner.register(myObject, 0, (r, m) -> {
        });
        assertEquals(myObject, ref.get());

        myObject = null;

        System.out.println("Create garbage and wait for reference became unreachable");
        assertTrue(
                generateGarbageAndWaitForCondition(GENERATE_GARBAGE_TIMEOUT, () -> ref.get() == null)
        );
    }

    @Test
    void cancel_cleaning() throws Exception {
        Object myObject = new Object();
        AtomicBoolean disposed = new AtomicBoolean(false);

        CleanableWeakReference<Object> ref = referenceCleaner.register(myObject, 0, (r, m) -> disposed.set(true));
        assertEquals(myObject, ref.get());

        ref.cancelCleaningOrder();
        myObject = null;
        System.out.println("Create garbage and wait for reference became unreachable");
        assertTrue(
                generateGarbageAndWaitForCondition(GENERATE_GARBAGE_TIMEOUT, () -> ref.get() == null)
        );
        assertFalse(disposed.get());
    }

    private boolean generateGarbageAndWaitForCondition(Duration duration, Supplier<Boolean> condition) throws Exception {
        ArrayList<Object> data = new ArrayList<>();
        long start = System.currentTimeMillis();

        while (!condition.get() && (System.currentTimeMillis() - start <= duration.toMillis())) {
            System.out.println("Running time: " + Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
            System.out.println("Occupied memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 + " Kb");

            for (int mb = 0; mb < 10; mb++) {
                for (int kb = 0; kb < 1024; kb++) {
                    int[] obj = new int[1024];
                    data.add(obj);
                }
            }
            data.clear();
            Thread.sleep(500);
        }
        return condition.get();
    }


}
