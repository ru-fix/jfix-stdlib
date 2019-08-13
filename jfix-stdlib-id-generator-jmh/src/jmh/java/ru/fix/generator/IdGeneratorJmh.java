package ru.fix.aggregating.profiler.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@State(Scope.Benchmark)
public class IdGeneratorJmh {
    final Synchronized sync = new Synchronized();
    @Benchmark
    public int sync() {
        return sync.doWork();
    }

    final RW rw = new RW();
    @Benchmark
    public int rw() {
        return rw.doWork();
    }
}

class Synchronized {
    AtomicInteger value = new AtomicInteger();

    int doWork() {
        int result = value.incrementAndGet();
        synchronized (this) {
            value.incrementAndGet();
        }
        return result;
    }
}

class RW {
    AtomicInteger value = new AtomicInteger();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    int doWork() {
        int result = value.incrementAndGet();

        if (result % 1000 == 0) {
            lock.writeLock().lock();
            value.incrementAndGet();
            lock.writeLock().unlock();
        } else {
            lock.readLock().lock();
            value.incrementAndGet();
            lock.readLock().unlock();
        }
        return result;
    }
}