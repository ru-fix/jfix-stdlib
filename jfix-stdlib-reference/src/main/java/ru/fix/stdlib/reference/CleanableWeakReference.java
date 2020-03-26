package ru.fix.stdlib.reference;

import java.lang.ref.WeakReference;

/**
 * Reference to the referent object.
 * Behaves as {@link WeakReference}
 */
public interface CleanableWeakReference<T>{
    /**
     * @return referent if it steal reachable or null
     */
    T get();

    /**
     * @return true if cleaning order canceled and {@link ReferenceCleaner} will not clean reference.
     *         false if {@link ReferenceCleaner} already acquired reference for cleaning
     *         and will invoke cleaning action in nearest future
     */
    boolean cancelCleaningOrder();
}
