package ru.fix.generator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import ru.fix.stdlib.id.generator.*;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;


@State(Scope.Benchmark)
public class IdGeneratorJmh {

    private final long START_OF_TIME = Clock.systemUTC().instant().toEpochMilli();
    private final BitsConfiguration bitsConfig = new BitsConfiguration(11, 43, 10);

    private final IdGenerator atomicGenerator = new AtomicIdGenerator(
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