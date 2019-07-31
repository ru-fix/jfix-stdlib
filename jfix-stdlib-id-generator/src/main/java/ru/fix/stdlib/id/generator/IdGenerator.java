package ru.fix.stdlib.id.generator;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class IdGenerator {

    private static final long START_OF_TIME =
            OffsetDateTime
                    .of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();

    Clock clock = Clock.systemUTC();
    AtomicInteger counter = new AtomicInteger();

    private final byte serverIdPart;

    public IdGenerator(Byte serverId) {
        this.serverIdPart = (byte) (serverId & 0x7F);
    }


    /**
     * Only for JUnit tests.
     *
     * @param serverId
     * @return
     */
    public static IdGenerator unsafeInstance(Byte serverId) {
        return new IdGenerator(serverId);
    }

    /**
     * Only for JUnit tests.
     *
     * @return
     */
    public static IdGenerator unsafeInstance() {
        return new IdGenerator((byte) new Random().nextInt(Byte.MAX_VALUE));
    }

    /**
     * Generate an number in the following order: [ ts | counter | server_id ] where:
     * ts - 43 bit (278 years)
     * counter - 13 bit (8k values)
     * server_id is 7 bit (128 values)
     * <p>for use it as db primary key</p>
     * <p>Key has 63 bit because we need to generate unsigned long</p>
     *
     * @return - generated number
     */
    public long nextVal() {
        long tsPart = ((clock.millis() - START_OF_TIME) & 0x07_ff_ff_ff_ff_ffL) << 20;
        long countPart = (counter.incrementAndGet() & 0x1f_ff) << 7;
        return tsPart | countPart | serverIdPart;
    }

    /**
     * выделяет timestamp из ключа
     * @param generatedId
     * @return
     */
    public static long extractTimeStamp(long generatedId) {
        long tsPart = generatedId >> 20;
        return START_OF_TIME+tsPart;
    }
}
