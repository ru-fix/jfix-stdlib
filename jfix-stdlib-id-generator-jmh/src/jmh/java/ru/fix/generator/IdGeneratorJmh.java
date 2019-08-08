package ru.fix.aggregating.profiler.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@State(Scope.Benchmark)
public class IdGeneratorJmh {

    final long timestamp = System.currentTimeMillis();

    Synchronized sync = new Synchronized();

    @Benchmark
    public int sync() {
        return sync.doWork();
    }

    RW rw = new RW();

    @Benchmark
    public int rw() {
        return rw.doWork();
    }
}

class Synchronized {
    int value;

    public synchronized int doWork() {
        return value++;
    }
}

class RW {
    int value;

    ReadWriteLock lock = new ReentrantReadWriteLock();

    public int doWork() {
        lock.readLock().lock();
        int result = value++;

        lock.readLock().unlock();


        if (result % 1000 == 0) {
            lock.writeLock().lock();
            value++;
            lock.writeLock().unlock();
        }

        return result;


    }
}