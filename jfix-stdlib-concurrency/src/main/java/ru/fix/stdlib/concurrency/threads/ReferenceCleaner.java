package ru.fix.stdlib.concurrency.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Use single static {@link ReferenceQueue} instance to track weak reference.
 * Maintain single {@link Thread} to run clean action.
 * {@link Thread} will be created on demand and destroyed if there no reference left to clean.
 */
public class ReferenceCleaner {
    private static final Logger log = LoggerFactory.getLogger(ReschedulableScheduler.class);

    private ReferenceCleaner() {
    }

    private static final ReferenceCleaner INSTANCE = new ReferenceCleaner();

    public static ReferenceCleaner getInstance(){
        return INSTANCE;
    }


    private ReferenceQueue referenceQueue = new ReferenceQueue();
    private Set<CleanableWeakReference> createdReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private AtomicReference<Thread> cleanerThread = new AtomicReference<>(null);

    private final class Reference<T, M> extends WeakReference<T> implements CleanableWeakReference {
        private final M meta;
        private final BiConsumer<CleanableWeakReference, M> cleaner;

        private Reference(T referent, M meta, BiConsumer<CleanableWeakReference, M> cleaner, ReferenceQueue referenceQueue) {
            super(referent, referenceQueue);
            this.meta = meta;
            this.cleaner = cleaner;
        }

        @Override
        public boolean cancelCleaningOrder() {
            return createdReferences.remove(this);
        }
    }

    /**
     * @param referent       object which reachability will be tracked through {@link ReferenceQueue}
     * @param metadata       will be passed to cleaning action
     * @param cleaningAction will be invoked when referent become weakly reachable
     * @param <T>            referent type
     * @param <M>            metadata type
     * @return reference interface that allows access to referent or cancel cleaning request
     */
    public <T, M> CleanableWeakReference<T> register(T referent, M metadata, BiConsumer<CleanableWeakReference<T>, M> cleaningAction) {
        final Reference ref = new Reference(referent, metadata, cleaningAction, referenceQueue);

        createdReferences.add(ref);
        ensureThreadExist();

        return ref;
    }

    private void ensureThreadExist() {
        if (cleanerThread.get() != null) return;

        CleanerThread newThread = new CleanerThread((thread)-> cleanerThread.compareAndSet(thread, null));

        if (cleanerThread.compareAndSet(null, newThread)) {
            newThread.start();
        }
    }

    private class CleanerThread extends Thread {
        private final Consumer<CleanerThread> onShutdown;

        public CleanerThread(Consumer<CleanerThread> onShutdown) {
            setName(ReferenceCleaner.class.getName());
            setDaemon(true);
            this.onShutdown = onShutdown;
        }

        @Override
        public void run() {
            try {
                while (!createdReferences.isEmpty() && !Thread.interrupted()) {
                    try {
                        Reference ref = (Reference) referenceQueue.remove();
                        if (ref != null) {
                            if (createdReferences.remove(ref)) {
                                ref.cleaner.accept(ref, ref.meta);
                            }
                        }
                    } catch (InterruptedException thr) {
                        //on interruption return
                        return;
                    } catch (Throwable thr) {
                        log.error("ReferenceCleaner cleaning thread failed", thr);
                    }
                }
            } finally {
                onShutdown.accept(this);
            }
        }
    }
}