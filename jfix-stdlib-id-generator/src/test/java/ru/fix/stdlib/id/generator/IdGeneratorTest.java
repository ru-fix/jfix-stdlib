package ru.fix.stdlib.id.generator;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdGeneratorTest {

    private static final Byte TEST_SERVER_ID = 23;
    private static final int TEST_COUNTER_VAL = 53;

    @Test
    public void shouldGenerateCorrectlyFormattedId() {
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(1450894554618L);

        AtomicInteger counter = new AtomicInteger(TEST_COUNTER_VAL - 1);

        IdGenerator generator = new IdGenerator(TEST_SERVER_ID, counter, clock);

        long generatedId = generator.nextVal();

        // 01/01/2015 0:00:00 equals   to 1420070400000 milliseconds from 01/01/1970 0:00:00
        //
        //
        // Wed Dec 23 2015 21:15:54 GMT+0300 (RTZ 2 (зима)) equals   to 1450894554618 milliseconds from 01/01/1970
        // 0:00:00
        //
        //
        // 1450894554618 - 1420070400000 = 30824154618;
        // unused            1 bit [                                                  0]
        // 23   =            7 bit [                                           00 10111]
        // 30824154618L =          [000 00000 11100 10110 10100 00110 10001 01111 11010]
        // 53    =          13 bit [                                    000 00001 10101]
        //
        //                                ||    ||
        //                                ||    ||
        //                                ||    ||
        //                         |||||||||    |||||||||
        //                           ||              ||
        //                             ||          ||
        //                               ||      ||
        //                                 ||  ||
        //                                   ||
        //
        //  00010 11100 00000 01110 01011 01010 00011 01000 10111 11101 00000 00011 0101

        assertEquals(
                "0000000001110010110101000011010001011111101000000001101010010111",
                fillZeros(Long.toBinaryString(generatedId))
        );

        long extractedTime = IdGenerator.extractTimeStamp(generatedId);
        assertEquals(extractedTime, 1450894554618L);

    }

    private String fillZeros(String in) {
        while (in.length() < 64) {
            in = "0".concat(in);
        }
        return in;
    }

    @Disabled("For testing key")
    public void test() {
        long s_id_mask = 0x7FL << 56;
        long ts_mask = 0x07_ff_ff_ff_ff_ffL << 13;
        long counter_mask = 0x1f_ff;

        long old_key = Long.valueOf("0001011100000000111001011010100001101000101111110100000000110101", 2);
        long newKey = ((old_key & s_id_mask) >> 56) + ((old_key & counter_mask) << 7) + ((old_key & ts_mask) << 7);

        System.out.println(Long.toBinaryString(newKey));
    }
}