package ru.fix.stdlib.id.generator

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import java.time.Clock
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
}