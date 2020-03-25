package ru.fix.stdlib.id.generator

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import java.lang.Exception
import java.time.*
import java.util.concurrent.atomic.AtomicLong


class AtomicIdGeneratorTest{

    @Test
    fun `counter overflow leads to increment time into the feature`(){
        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(0b111000)

        val g = AtomicIdGenerator(BitsConfiguration(40,2,2), 0, 0b10, clock)
        //timestamp | counter | serverId
        assertEquals(0b111000_01_10, g.nextId())
        assertEquals(0b111000_10_10, g.nextId())
        assertEquals(0b111000_11_10, g.nextId())
        assertEquals(0b111001_00_10, g.nextId())
    }

    @Test
    fun `current time update leads to update timestamp part in id`(){
        val clock = Mockito.mock(Clock::class.java)
        val clockAnswer = AtomicLong()
        `when`(clock.millis()).thenAnswer { clockAnswer.get() }

        clockAnswer.set(0b111001)
        val g = AtomicIdGenerator(BitsConfiguration(40,2,2), 0, 0b10, clock)
        //timestamp | counter | serverId
        assertEquals(0b111001_01_10, g.nextId())

        clockAnswer.set(0b111011)
        assertEquals(0b111011_00_10, g.nextId())
    }

    @Test
    fun `actual time is currentTime minus startOfTime`(){
        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(0b1100110)

        val g = AtomicIdGenerator(BitsConfiguration(40,2,2), 0b1100000, 0b10, clock)
        //timestamp | counter | serverId
        assertEquals(0b0000110_01_10, g.nextId())
    }

    @Test
    fun `error when create generator with server part value greater than max`() {
        val bits = BitsConfiguration(serverPartBits = 2, timePartBits = 43, counterPartBits = 19)

        Assertions.assertThrows(Exception::class.java) {
            AtomicIdGenerator(bits, 0, 16, Clock.systemUTC())
        }
    }

    @Test
    fun `error when create generator with start of time greater than current`() {
        val bits = BitsConfiguration(serverPartBits = 2, timePartBits = 43, counterPartBits = 19)

        Assertions.assertThrows(Exception::class.java) {
            AtomicIdGenerator(bits, System.currentTimeMillis() + 10000, 16, Clock.systemUTC())
        }
    }

    @Test
    fun `generate unique id`() {
        val startOfTime = OffsetDateTime
                .of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli()

        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1450894554618L)

        val TEST_SERVER_ID = 23L
        val TEST_COUNTER_VAL = 53L

        val counter = AtomicLong(TEST_COUNTER_VAL - 1)
        val bits = BitsConfiguration(timePartBits = 43, counterPartBits = 13, serverPartBits = 7)

        val generator = AtomicIdGenerator(bits, startOfTime, TEST_SERVER_ID, clock)

        //set counter value to TEST_COUNTER_VAL
        for (i in 1 until TEST_COUNTER_VAL)
            generator.nextId()


        val generatedId = generator.nextId()

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
    }

    @Test
    fun `generate many ids per unit time when counter is 2 then all ids must be unique`() {

        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1L)

        val idList = mutableListOf<Long>()

        val bits = BitsConfiguration(timePartBits = 42, counterPartBits = 2, serverPartBits = 19)

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
        val idGenerator = AtomicIdGenerator(bits,0, 1, clock)

        for (i in 1..10) {
            idList.add(idGenerator.nextId())
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
        val numberOfIdsGeneratedPerJob = 100

        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(1L)
        val bits = BitsConfiguration(serverPartBits = 19, timePartBits = 42, counterPartBits = 2)

        val idGenerator = AtomicIdGenerator(bits,0, 1, clock)

        val jobs = mutableListOf<Deferred<List<Long>>>()
        for (job in 1..numberOfJobs) {

            val task = GlobalScope.async {
                val idList = mutableListOf<Long>()
                for (i in 1..numberOfIdsGeneratedPerJob) {
                    idList.add(idGenerator.nextId())
                }
                return@async idList
            }

            jobs.add(task)
        }

        val allGeneratedIds = mutableSetOf<Long>()
        jobs.forEach {
            val idList = it.await()

            assertEquals(idList.size, idList.toSet().size) {
                "Ids generated by job not unique"
            }

            allGeneratedIds.addAll(idList)
        }

        assertEquals(allGeneratedIds.size, numberOfJobs * numberOfIdsGeneratedPerJob) {
            "Ids generated by all jobs not unique"
        }
    }

    @Test
    fun `id time must be reset if current time is higher`() {

        val clock = Mockito.mock(Clock::class.java)
        val clockAnswer = AtomicLong()
        `when`(clock.millis()).thenAnswer { clockAnswer.get() }

        val idList = mutableListOf<Long>()

        val bits = BitsConfiguration(timePartBits = 42, counterPartBits = 1, serverPartBits = 20)

        clockAnswer.set(1)

        // timestamp = 42 bit, value = [000 00000 00000 00000 00000 00000 00000 00000 00001]
        // counter   =  1 bit, value = [0], [1]
        // server    = 20 bit, value = [00000 00000 00000 00001]
        //
        // start with 1 in time and 1 in counter part
        // 0000 00000 00000 00000 00000 00000 00000 00000 00011 00000 00000 00000 00001
        // increment counter lead to overflow that triggers increment time part
        // 0000 00000 00000 00000 00000 00000 00000 00000 00100 00000 00000 00000 00001
        // current time  update will update time part and reset counter
        // 0000 00000 00000 00000 00000 00000 00000 00000 10000 00000 00000 00000 00001
        val idGenerator = AtomicIdGenerator(bits,0, 1, clock)

        idList.add(idGenerator.nextId())
        idList.add(idGenerator.nextId())

        clockAnswer.set(8)
        idList.add(idGenerator.nextId())


        assertEquals(idList.toSet().size, 3) { "Generated values are not unique" }

        assertEquals("0000000000000000000000000000000000000000001100000000000000000001", toBinaryString(idList[0]))
        assertEquals("0000000000000000000000000000000000000000010000000000000000000001", toBinaryString(idList[1]))
        assertEquals("0000000000000000000000000000000000000001000000000000000000000001", toBinaryString(idList[2]))
    }

    @Test
    fun `ids must be unique if time parts bit are low`() {
        val bits = BitsConfiguration(
            timePartBits = 20,
            counterPartBits = 10,
            serverPartBits = 20
        )
        val idGenerator = AtomicIdGenerator(
            bitsConfig = bits,
            startOfTime = Instant.now().minus(Duration.ofDays(1)).toEpochMilli(),
            serverId = 1,
            clock = Clock.fixed(Instant.now(), ZoneId.systemDefault())
        )

        val generatedIds = (1..5).map { idGenerator.nextId() }
        assertEquals(generatedIds.toSet().size, generatedIds.size)
    }

    @Test
    fun `extracting parts from generated id`() {
        val clock = Mockito.mock(Clock::class.java)
        `when`(clock.millis()).thenReturn(0b111000)

        val g = AtomicIdGenerator(BitsConfiguration(40,3,2), 0, 0b10, clock)

        for (i in 1..5) {
            val id = g.nextId()

            // time must be the same
            assertEquals(0b111000, g.extractTimePart(id))
            // counter must increase
            assertEquals(i.toLong(), g.extractCounterPart(id))
            // server id must be the same
            assertEquals(0b10, g.extractServerIdPart(id))
        }
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