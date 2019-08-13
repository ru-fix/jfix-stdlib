package ru.fix.stdlib.id.generator

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import java.time.Clock
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong

private const val TEST_SERVER_ID = 23L
private const val TEST_COUNTER_VAL = 53L

class KotlinIdGeneratorTest {

    @Test
    fun `generate unique id`() {
        val startOfTime = OffsetDateTime
                .of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli()

        val clock = mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1450894554618L)

        val counter = AtomicLong(TEST_COUNTER_VAL - 1)
        val bytePart = ByteConfiguration(serverPartBytes = 7, timePartBytes = 43, counterPartBytes = 13)

        val generator = KotlinIdGenerator(bytePart, startOfTime, TEST_SERVER_ID, clock, counter)

        val generatedId = generator.nextId2()

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
        // 30824154618L =   43 bit [000 00000 11100 10110 10100 00110 10001 01111 11010]
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
        //  0001 01110 00000 00111 00101 10101 00001 10100 01011 11110 10000 00001 10101

        assertEquals("0000000001110010110101000011010001011111101000000001101010010111", toBinaryString(generatedId))

        val extractedTime = IdGenerator.extractTimeStamp(generatedId)
        assertEquals(extractedTime, 1450894554618L)
    }

    @Test
    fun `generate many ids per unit time when counter is 2 then all ids must be unique`() {

        val clock = mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1L)

        val idList = mutableListOf<Long>()

        val byteConfiguration = ByteConfiguration(serverPartBytes = 19, timePartBytes = 42, counterPartBytes = 2)

        // timestamp = 43 bit, value = [000 00000 00000 00000 00000 00000 00000 00000 00001]
        // counter   =  2 bit, value = [00], [01], [10], [11]
        // server    = 19 bit, value = [0000 00000 00000 00001]
        //
        // 0000 00000 00000 00000 00000 00000 00000 00000 00010 10000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 00011 00000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 00011 10000 00000 00000 00001
        //
        // 0000 00000 00000 00000 00000 00000 00000 00000 00100 00000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 00100 10000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 00101 00000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 00101 10000 00000 00000 00001
        //
        // 0000 00000 00000 00000 00000 00000 00000 00000 01100 00000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 01100 10000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 01101 00000 00000 00000 00001
        // 0000 00000 00000 00000 00000 00000 00000 00000 01101 10000 00000 00000 00001
        val idGenerator = KotlinIdGenerator(byteConfiguration,0, 1, clock, AtomicLong(0))

        for (i in 1..10) {
            idList.add(idGenerator.nextId2())
        }

        assertEquals(idList.toSet().size, 10) { "Generated values are not unique" }

        assertEquals("0000000000000000000000000000000000000000001010000000000000000001", toBinaryString(idList[0]))
        assertEquals("0000000000000000000000000000000000000000001100000000000000000001", toBinaryString(idList[1]))
        assertEquals("0000000000000000000000000000000000000000001110000000000000000001", toBinaryString(idList[2]))

        assertEquals("0000000000000000000000000000000000000000010000000000000000000001", toBinaryString(idList[3]))
        assertEquals("0000000000000000000000000000000000000000010010000000000000000001", toBinaryString(idList[4]))
        assertEquals("0000000000000000000000000000000000000000010100000000000000000001", toBinaryString(idList[5]))
        assertEquals("0000000000000000000000000000000000000000010110000000000000000001", toBinaryString(idList[6]))

        assertEquals("0000000000000000000000000000000000000000011000000000000000000001", toBinaryString(idList[7]))
        assertEquals("0000000000000000000000000000000000000000011010000000000000000001", toBinaryString(idList[8]))
        assertEquals("0000000000000000000000000000000000000000011100000000000000000001", toBinaryString(idList[9]))
    }

    @RepeatedTest(100)
    fun `all ids must be unique when generate from many parallel jobs`() = runBlocking {
        val numberOfJobs = 100
        val numberOfIdsGeneratedPerJob = 10

        val clock = mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1L)
        val byteConfiguration = ByteConfiguration(serverPartBytes = 19, timePartBytes = 42, counterPartBytes = 2)

        val idGenerator = KotlinIdGenerator(byteConfiguration,0, 1, clock, AtomicLong(0))

        val jobs = mutableListOf<Deferred<List<Long>>>()
        for (job in 1..numberOfJobs) {

            val task = GlobalScope.async {
                val idList = mutableListOf<Long>()
                for (i in 1..numberOfIdsGeneratedPerJob) {
                    idList.add(idGenerator.nextId2())
                }
                return@async idList
            }

            jobs.add(task)
        }

        val allGeneratedIds = mutableSetOf<Long>()
        jobs.forEach {
            val idList = it.await()

            assertEquals( idList.size, idList.toSet().size) {
                "Ids generated by job not unique"
            }

            allGeneratedIds.addAll(idList)
        }

        assertEquals(allGeneratedIds.size, numberOfJobs * numberOfIdsGeneratedPerJob) {
            "Ids generated by all jobs not unique"
        }
    }

    @Test
    fun kotlinShiftTest() {

        val count = TEST_COUNTER_VAL

        println(count)
        println(0x1f_ff)

        println()

        println(count.toString(2))
        println(0x1f_ff.toString(2))

        println()

        println(count and 0x1f_ff)
        println(count and 8191)
        println((count and 0x1f_ff).toString(2))

        println()

        println((count and 0x1f_ff) shl 7)
        println(((count and 0x1f_ff) shl 7).toString(2))
    }

    private fun toBinaryString(value: Long): String {
        return fillZeros(value.toString(2))
    }

    private fun fillZeros(input: String): String {
        var result = input
        while (result.length < 64) {
            result = "0$result"
        }
        return result
    }
}