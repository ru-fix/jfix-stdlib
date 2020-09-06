package ru.fix.stdlib.concurrency.events

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * All events passed through [publishEvent] will be merged by [reduceFunction] into single accumulator.
 * When [extractAccumulatedValue] invoked, it will extract value from accumulator.
 *
 * If producers publishes events [publishEvent] with rate 1 per millisecond
 * and consumer invokes [extractAccumulatedValue] every second
 * then each 1000 events will be aggregated by [reduceFunction]
 * and consumer will see new aggregated value with rate 1 per second.
 *
 * ```
 * val events = Array(500) { it } //numbers from 1 to 500
 * val accumulator = ReducingEventAccumulator<Int, MutableList<Int>>(
 *   reduceFunction = { accumulator, event ->
 *     accumulator?.apply { add(event) } ?: mutableListOf(event) //accumulate ints in list
 *   })
 * val consumer = Executors.newSingleThreadExecutor()
 * consumer.execute {
 *   while(!accumulator.isClosedAndEmpty()) {
 *     Thread.sleep(100) //simulate slow event consuming
 *     println(it.size) //print received list size
 *   }
 * }
 * for(event in events) {
 *      Thread.sleep(1) //sending event every 1 ms
 *      accumulator.publishEvent(event)
 * }
 * Thread.sleep(3_000) //block thread to see what will be send to console
 * accumulator.close()
 * consumer.shutdown()
 * ```
 * It will produce something like this: `1 93 91 91 93 92 39`.
 * The consumer received about 7 aggregated events instead of 500
 * */
class ReducingEventAccumulator<ReceivingEventT, AccumulatorT>(
        /**
         * Merge new event to existing accumulator.
         * First time accumulators AccumulatorT? value will be null.
         * Frequently invoked for each new event in publisher thread.
         * Should be non blocking and lightweight.
         */
        private val reduceFunction: (accumulator: AccumulatorT?, newEvent: ReceivingEventT) -> AccumulatorT
) : AutoCloseable {

    private var closed = false
    private var accumulator: AccumulatorT? = null

    private val lock = ReentrantLock()
    private val eventPublishedOrAccumulatorClosed = lock.newCondition()

    /**
     * Adds event to accumulator.
     * Reduces events with `reduceFunction` if accumulator is not empty.
     */
    fun publishEvent(event: ReceivingEventT) = lock.withLock {
        if (!closed) {
            accumulator = reduceFunction.invoke(accumulator, event)
            eventPublishedOrAccumulatorClosed.signalAll()
        }
    }


    /**
     * Extract value from accumulator.
     * Returns null if accumulator is empty.
     * Blocks until new event is published or accumulator is closed or extractTimeoutMs expires.
     *
     * Invoke this function when you ready to proceed next [AccumulatorT] event.
     * Thread-safe.
     *
     * @return [AccumulatorT] if at least one [ReceivingEventT] was published through [publishEvent] function
     * since previous extraction or null
     * */
    fun extractAccumulatedValue(): AccumulatorT? =
            extractAccumulatedValue(Long.MAX_VALUE)

    /**
     * Extract value from accumulator.
     * Returns null if accumulator is empty.
     * Blocks until new event is published or accumulator is closed or extractTimeoutMs expires.
     *
     * Invoke this function when you ready to proceed next [AccumulatorT] event.
     * Thread-safe.
     *
     * @param extractTimeoutMs time in milliseconds to wait for new event.
     *                         Use [Long.MAX_VALUE] to specify infinite timeout.
     *
     * @return [AccumulatorT] if at least one [ReceivingEventT] was published through [publishEvent] function
     * since previous extraction or null
     * */
    fun extractAccumulatedValue(extractTimeoutMs: Long): AccumulatorT? = lock.withLock {
        val startTime = System.currentTimeMillis()
        while (true) {
            if (accumulator != null) {
                return accumulator.also { accumulator = null }
            }

            if(extractTimeoutMs == Long.MAX_VALUE){
                eventPublishedOrAccumulatorClosed.await()

            } else {
                val timeLeft = Math.max(0, extractTimeoutMs - System.currentTimeMillis() - startTime)
                if (timeLeft <= 0)
                    return null

                eventPublishedOrAccumulatorClosed.await(timeLeft, TimeUnit.MILLISECONDS)
            }
        }
        throw IllegalStateException()
    }


    override fun close(): Unit = lock.withLock {
        closed = true
        eventPublishedOrAccumulatorClosed.signal()
    }

    /**
     * @return true if accumulator is closed
     * */
    fun isClosed() = lock.withLock {
        return@withLock closed
    }

    /**
     * @return true if [close] was invoked and last [ReceivingEventT] received before closing
     * was accumulated in [AccumulatorT] and extracted by [extractAccumulatedValue] function
     * */
    fun isClosedAndEmpty() = lock.withLock {
        return@withLock closed && accumulator == null
    }

    companion object {
        @JvmStatic
        fun <EventT> createLastEventWinAccumulator() =
                ReducingEventAccumulator { _: EventT?, event: EventT -> event }
    }
}