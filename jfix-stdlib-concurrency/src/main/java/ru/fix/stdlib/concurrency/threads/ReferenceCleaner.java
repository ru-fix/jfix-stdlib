package ru.fix.stdlib.concurrency.threads;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Use single static {@link ReferenceQueue} instance to track weak reference.
 * Maintain single {@link Thread} to run clean action.
 * {@link Thread} will be created on demand and destroyed if there no reference left to clean.
 */
public class ReferenceCleaner {

    private ReferenceCleaner() {
    }

    private static ReferenceQueue referenceQueue = new ReferenceQueue();
    private static Set<DisposableWeakReference> createdReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static AtomicReference<Thread> cleanerThread = new AtomicReference<>(null);

    private static final class DisposableWeakReference<T, M> extends WeakReference<T> {
        public final M meta;
        public final Consumer<M> cleaner;

        public DisposableWeakReference(T referent, M meta, Consumer<M> cleaner, ReferenceQueue referenceQueue) {
            super(referent, referenceQueue);
            this.meta = meta;
            this.cleaner = cleaner;
        }
    }

    public static <T, M> void register(T referent, M disposerMetadata, Consumer<M> disposer) {
        createdReferences.add(new DisposableWeakReference(referent, disposerMetadata, disposer, referenceQueue));
        ensureThreadExist();
    }

    private static void ensureThreadExist() {
        if (cleanerThread.get() != null) return;

        Thread newThread = new Thread(ReferenceCleaner::processQueueRoutine);
        newThread.setName(ReferenceCleaner.class.getName());
        newThread.setDaemon(true);

        if (cleanerThread.compareAndSet(null, newThread)) {
            newThread.start();
        }
    }

    private static void processQueueRoutine() {
        try {
            while (!createdReferences.isEmpty() && !Thread.interrupted()) {
                DisposableWeakReference ref = (DisposableWeakReference) referenceQueue.remove();
                if (ref != null) {
                    createdReferences.remove(ref);
                    ref.cleaner.accept(ref.meta);
                }
            }
        } catch (InterruptedException thr) {
            //on interruption return
        }
    }
}