package ru.fix.stdlib.concurrency.events

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

internal class ReducingEventAccumulatorTest {

    @Test
    fun `WHEN publish single time THEN handler invoked one time`() {
        val accumulator = ReducingEventAccumulator.lastEventWinAccumulator<Any>()
        assertNull(accumulator.extractAccumulatedValueOrNull())
        accumulator.publishEvent(Any())
        assertNotNull(accumulator.extractAccumulatedValueOrNull())
        assertNull(accumulator.extractAccumulatedValueOrNull())
    }

    @Test
    fun `WHEN publish several events at once THEN all events reached by one or two reducedEvents`() {
        val eventsQuantity = 50
        val events = Array(eventsQuantity) { it } //numbers from 1 to eventsQuantity
        val sumAccumulator = defaultIntSumAccumulator()
        val awaitThreadsBeforePublishingLatch = CountDownLatch(eventsQuantity)
        val awaitThreadsAfterPublishingLatch = CountDownLatch(eventsQuantity)

        val executor = Executors.newFixedThreadPool(eventsQuantity)
        for (event in events) {
            executor.execute {
                awaitThreadsBeforePublishingLatch.countDown()
                awaitThreadsBeforePublishingLatch.await()

                sumAccumulator.publishEvent(event)

                awaitThreadsAfterPublishingLatch.countDown()
                awaitThreadsAfterPublishingLatch.await()
            }
        }
        awaitThreadsAfterPublishingLatch.await()

        val firstAccumulatedValue = sumAccumulator.extractAccumulatedValueOrNull()!!
        val secondAccumulatedValue = sumAccumulator.extractAccumulatedValueOrNull()
        assertEquals(events.sum(), firstAccumulatedValue + (secondAccumulatedValue ?: 0))

        executor.shutdown()
    }


    @Test
    fun `WHEN accumulator was closed THEN not reduces events anymore AND returns 'closed' boolean accordingly`() {
        val sumAccumulator = defaultIntSumAccumulator()
        assertFalse(sumAccumulator.isClosed())
        assertFalse(sumAccumulator.isClosedAndEmpty())

        val eventBeforeClosing = 235
        sumAccumulator.publishEvent(eventBeforeClosing)

        sumAccumulator.close()

        assertTrue(sumAccumulator.isClosed())
        assertFalse(sumAccumulator.isClosedAndEmpty())

        sumAccumulator.publishEvent(6345) // won't be reduced
        assertEquals(eventBeforeClosing, sumAccumulator.extractAccumulatedValueOrNull())

        assertTrue(sumAccumulator.isClosed())
        assertTrue(sumAccumulator.isClosedAndEmpty())
    }


    @Test
    fun `WHEN receiving and reduced event types are different AND custom reduce function provided THEN it used`() {
        val events = Array(50) { it }
        val listAccumulator = ReducingEventAccumulator<Int, MutableList<Int>>(
                reduceFunction = { accumulator, event ->
                    accumulator?.apply { add(event) } ?: mutableListOf(event)
                }
        )

        val executor = Executors.newSingleThreadExecutor()
        executor.execute {
            for (event in events) {
                listAccumulator.publishEvent(event)
            }
        }

        val allReceivedEvents = mutableListOf<Int>()
        var nextReceivedEvents: MutableList<Int>? = mutableListOf()
        while (nextReceivedEvents != null) {
            allReceivedEvents.addAll(nextReceivedEvents)
            nextReceivedEvents = listAccumulator.extractAccumulatedValueOrNull()
        }

        assertEquals(events.toSet(), allReceivedEvents.toSet())

        executor.shutdown()
    }

    @Test
    fun `WHEN receiveReducedEventsUntilClosedAndEmpty used THEN not extracted events will be received after closing`() {
        val eventBeforeClosing = 1

        val sumAccumulator = defaultIntSumAccumulator()

        sumAccumulator.publishEvent(eventBeforeClosing)
        sumAccumulator.close()
        sumAccumulator.publishEvent(2)

        var receivedEventSum = 0
        sumAccumulator.receiveReducedEventsUntilClosedAndEmpty {
            receivedEventSum += it
        }
        assertEquals(eventBeforeClosing, receivedEventSum)
    }


    @Test
    fun `WHEN receiveReducedEventsUntilClosed used THEN not extracted events won't be received after closing`() {

        val sumAccumulator = defaultIntSumAccumulator()

        sumAccumulator.publishEvent(1)
        sumAccumulator.close()
        sumAccumulator.publishEvent(2)

        var receivedEventSum = 0
        sumAccumulator.receiveReducedEventsUntilClosed {
            receivedEventSum += it
        }
        assertEquals(0, receivedEventSum)
    }


    private fun defaultIntSumAccumulator() =
            ReducingEventAccumulator<Int, Int> { accumulatedEvent, event -> event + (accumulatedEvent ?: 0) }

}