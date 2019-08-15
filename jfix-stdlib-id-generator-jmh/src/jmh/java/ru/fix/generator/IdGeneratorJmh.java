package ru.fix.generator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import ru.fix.stdlib.id.generator.*;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;


@State(Scope.Benchmark)
public class IdGeneratorJmh {

    final long START_OF_TIME = Clock.systemUTC().instant().toEpochMilli();
    final BitsConfiguration bitsConfig = new BitsConfiguration(11, 43, 10);

    final IdGenerator synchronizedIdGenerator = new SynchronizedIdGenerator(
            bitsConfig,
            START_OF_TIME,
            1,
            Clock.systemUTC(),
            0
    );

    @Benchmark
    public long sync() {
        return synchronizedIdGenerator.nextId();
    }

    final IdGenerator rwLockIdGenerator = new ReadWriteLockIdGenerator(
            bitsConfig,
            START_OF_TIME,
            1,
            Clock.systemUTC(),
            new AtomicLong(0)
    );

    @Benchmark
    public long rw() {
        return rwLockIdGenerator.nextId();
    }

    final IdGenerator atomicGenerator = new AtomicIdGenerator(
            bitsConfig,
            START_OF_TIME,
            1,
            Clock.systemUTC());


    @Benchmark
    public long atomic() {
        return atomicGenerator.nextId();
    }

    final IdGenerator unsafeAtomicIncGenerator = new IdGenerator() {
        AtomicLong counter = new AtomicLong();
        @Override
        public long nextId() {
            return (System.currentTimeMillis() << (bitsConfig.getCounterPartBits() + bitsConfig.getServerPartBits())) |
                    (counter.incrementAndGet() & bitsConfig.getCounterPartMask() << bitsConfig.getServerPartBits()) |
                    1 & bitsConfig.getServerPartMask();
        }
    };

    @Benchmark
    public long unsafeAtomicIncrement() {
        return atomicGenerator.nextId();
    }
}