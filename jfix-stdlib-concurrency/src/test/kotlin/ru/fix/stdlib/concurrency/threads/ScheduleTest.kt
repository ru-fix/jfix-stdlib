package ru.fix.stdlib.concurrency.threads

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ScheduleTest {

    @Test
    fun `schedule equality`() {
        val sc1 = Schedule.withDelay(10_000L)
        val sc2 = Schedule.withDelay(10_000L)
        val sc3 = Schedule.withDelay(20_000L)
        val sc4 = Schedule.withRate(5_000L)
        val sc5 = Schedule.withRate(5_000L)
        val sc6 = Schedule.withRate(10_000L)

        assertFalse(sc1 === sc2)
        assertTrue(sc1 == sc2)
        assertFalse(sc1 == sc3)

        assertFalse(sc1 == sc6)

        assertFalse(sc4 === sc5)
        assertTrue(sc4 == sc5)
        assertFalse(sc4 == sc6)

    }
}
