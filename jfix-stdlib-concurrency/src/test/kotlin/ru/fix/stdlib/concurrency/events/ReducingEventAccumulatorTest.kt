package ru.fix.stdlib.concurrency.events

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.lang.Thread.sleep
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

internal class ReducingEventAccumulatorTest {

    @Test
    fun `WHEN publish single time THEN handler invoked one time`() {
        val accumulator = ReducingEventAccumulator.createLastEventWinAccumulator<Any>()
        assertNull(accumulator.extractAccumulatedValue(1000))
        accumulator.publishEvent(Any())
        assertNotNull(accumulator.extractAccumulatedValue())
        assertNull(accumulator.extractAccumulatedValue(1000))
    }

    @Test
    fun `WHEN publish several events at once THEN all events reached by one or two reducedEvents`() {
        val eventsQuantity = 50
        val events = Array(eventsQuantity) { it } //numbers from 1 to eventsQuantity
        val sumAccumulator = createIntSumAccumulator()
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

        val firstAccumulatedValue = sumAccumulator.extractAccumulatedValue(1000)!!
        val secondAccumulatedValue = sumAccumulator.extractAccumulatedValue(1000)
        assertEquals(events.sum(), firstAccumulatedValue + (secondAccumulatedValue ?: 0))

        executor.shutdown()
    }


    @Test
    fun `WHEN accumulator was closed THEN not reduces events anymore AND returns 'closed' boolean accordingly`() {
        val sumAccumulator = createIntSumAccumulator()
        assertFalse(sumAccumulator.isClosed())
        assertFalse(sumAccumulator.isClosedAndEmpty())

        val eventBeforeClosing = 235
        sumAccumulator.publishEvent(eventBeforeClosing)

        sumAccumulator.close()

        assertTrue(sumAccumulator.isClosed())
        assertFalse(sumAccumulator.isClosedAndEmpty())

        sumAccumulator.publishEvent(6345) // won't be reduced
        assertEquals(eventBeforeClosing, sumAccumulator.extractAccumulatedValue())

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
            nextReceivedEvents = listAccumulator.extractAccumulatedValue(1000)
        }

        assertEquals(events.toSet(), allReceivedEvents.toSet())

        executor.shutdown()
    }

    @Test
    fun `WHEN receiveReducedEventsUntilClosedAndEmpty used THEN not extracted events will be received after closing`() {
        val eventBeforeClosing = 1

        val sumAccumulator = createIntSumAccumulator()

        sumAccumulator.publishEvent(eventBeforeClosing)
        sumAccumulator.close()
        sumAccumulator.publishEvent(2)

        var receivedEventSum = 0

        while(!sumAccumulator.isClosedAndEmpty()){
            sumAccumulator.extractAccumulatedValue()?.let {
                receivedEventSum += it
            }
        }
        assertEquals(eventBeforeClosing, receivedEventSum)
    }


    @Test
    fun `WHEN receiveReducedEventsUntilClosed used THEN not extracted events won't be received after closing`() {

        val sumAccumulator = createIntSumAccumulator()

        sumAccumulator.publishEvent(1)
        sumAccumulator.close()
        sumAccumulator.publishEvent(2)

        var receivedEventSum = 0

        while(!sumAccumulator.isClosed()){
            sumAccumulator.extractAccumulatedValue()?.let {
                receivedEventSum += it
            }
        }
        assertEquals(0, receivedEventSum)
    }


    private fun createIntSumAccumulator() =
            ReducingEventAccumulator<Int, Int> { accumulatedEvent, event -> event + (accumulatedEvent ?: 0) }

    @Test
    fun `closing awakens threads blocked on extract method`(){
        val sumAccumulator = createIntSumAccumulator()

        val extractMethodUnblocked = AtomicReference<Boolean>(false)
        Executors.newSingleThreadExecutor().submit {
            val value = sumAccumulator.extractAccumulatedValue(TimeUnit.MINUTES.toMillis(1))
            extractMethodUnblocked.set(true)
        }
        sleep(TimeUnit.SECONDS.toMillis(1))
        extractMethodUnblocked.get().shouldBeFalse()

        sumAccumulator.close()
        await().atMost(15, TimeUnit.SECONDS).until { extractMethodUnblocked.get() == true }
    }

    @Test
    fun `extractAccumulatedValue blocks no more that for given timeout`() {
        val sumAccumulator = createIntSumAccumulator()

        val extractMethodUnblocked = AtomicReference<Boolean>(false)
        Executors.newSingleThreadExecutor().submit {
            val value = sumAccumulator.extractAccumulatedValue(TimeUnit.SECONDS.toMillis(3))
            extractMethodUnblocked.set(true)
        }
        sleep(TimeUnit.SECONDS.toMillis(1))
        extractMethodUnblocked.get().shouldBeFalse()

        await().atMost(5, TimeUnit.SECONDS).until { extractMethodUnblocked.get() == true }

    }
}