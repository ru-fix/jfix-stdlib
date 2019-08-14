package ru.fix.generator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import ru.fix.stdlib.id.generator.BitsConfiguration;
import ru.fix.stdlib.id.generator.IdGenerator;
import ru.fix.stdlib.id.generator.ReadWriteLockIdGenerator;
import ru.fix.stdlib.id.generator.SynchronizedIdGenerator;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;


@State(Scope.Benchmark)
public class IdGeneratorJmh {
    private final IdGenerator synchronizedIdGenerator = new SynchronizedIdGenerator(
            new BitsConfiguration(11, 43, 10),
            Clock.systemUTC().instant().toEpochMilli(),
            1,
            Clock.systemUTC(),
            0
    );

    @Benchmark
    public long sync() {
        return synchronizedIdGenerator.nextId();
    }

    private final IdGenerator rwLockIdGenerator = new ReadWriteLockIdGenerator(
            new BitsConfiguration(11, 43, 10),
            Clock.systemUTC().instant().toEpochMilli(),
            1,
            Clock.systemUTC(),
            new AtomicLong(0)
    );

    @Benchmark
    public long rw() {
        return rwLockIdGenerator.nextId();
    }
}