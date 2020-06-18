package ru.fix.stdlib.concurrency.events

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private const val DEFAULT_EXTRACT_TIMEOUT_MS = 1_000L

/**
 * All events passed through [publishEvent] function before [extractAccumulatedValueOrNull] invoked
 * will be accumulated in single butch of events by given [reduceFunction].
 * When [extractAccumulatedValueOrNull] invoked, it will receive single butch of events,
 * and new butch will be started by next [publishEvent] invocation.
 *
 * Therefore, if [ReceivingEventT] occurred 500 times with breaks in 1 millisecond,
 * and the handler calling the function "extractAccumulatedValueOrNull" spends 100 milliseconds to process one event,
 * it will not process 500 [ReceivingEventT], but 6-7 [AccumulatedEventT]
 *
 * Here is example:
 * ```
 * val events = Array(500) { it } //numbers from 1 to 500
 * val accumulator = ReducingEventAccumulator<Int, MutableList<Int>>(
 * reduceFunction = { accumulator, event ->
 *      accumulator?.apply { add(event) } ?: mutableListOf(event) //accumulate ints in list
 * })
 * val consumer = Executors.newSingleThreadExecutor()
 * consumer.execute {
 *      accumulator.receiveReducedEventsUntilClosed {
 *          Thread.sleep(100) //simulate slow event consuming
 *          println(it.size) //print received list size
 *      }
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
 * The consumer received about 7 events instead of 500, and all 500 numbers was reached
 * */
class ReducingEventAccumulator<ReceivingEventT, AccumulatedEventT>(
        /**
         * Frequently invoked for each new event to accumulate it in butch of events.
         * AccumulatedEvent is going to be null at the start of new butch
         * */
        private val reduceFunction: (accumulatedEvent: AccumulatedEventT?, newEvent: ReceivingEventT) -> AccumulatedEventT,
        /**
         * Time to wait until [AccumulatedEventT] will appear for extracting
         * */
        private val extractTimeoutMs: Long = DEFAULT_EXTRACT_TIMEOUT_MS
) : AutoCloseable {

    private val isClosed = AtomicBoolean(false)

    private val awaitingEventQueue = ArrayBlockingQueue<AccumulatedEventT>(1)

    private val reducingEventsLock = ReentrantLock()

    /**
     * Invoke this function for each new event. Thread-safe.
     * */
    fun publishEvent(event: ReceivingEventT) {
        if (!isClosed.get()) {
            reducingEventsLock.withLock {
                val oldAccumulatedEvent: AccumulatedEventT? = awaitingEventQueue.poll()
                val newAccumulatedEvent: AccumulatedEventT = reduceFunction.invoke(oldAccumulatedEvent, event)
                awaitingEventQueue.put(newAccumulatedEvent)
            }
        }
    }

    /**
     * Invoke this function when you ready to proceed next [AccumulatedEventT]. Thread-safe.
     * Waits given [extractTimeoutMs] for [publishEvent] invocation
     * if none [ReceivingEventT] was published since last extraction.
     *
     * @return [AccumulatedEventT] if at least one [ReceivingEventT] was passed through [publishEvent] function
     * since last extraction or during [extractTimeoutMs] after this function invocation, else null
     * */
    fun extractAccumulatedValueOrNull(): AccumulatedEventT? =
            awaitingEventQueue.poll(extractTimeoutMs, TimeUnit.MILLISECONDS)

    override fun close() = isClosed.set(true)

    /**
     * @return true if [close] was invoked
     * */
    fun isClosed() = isClosed.get()

    /**
     * @return true if [close] was invoked and last [ReceivingEventT] received before closing
     * was accumulated in [AccumulatedEventT] and extracted by [extractAccumulatedValueOrNull] function
     * */
    fun isClosedAndEmpty() = isClosed() && !reducingEventsLock.isLocked && awaitingEventQueue.isEmpty()

    /**
     * Use it if you want to proceed all events, passed through [publishEvent] function before [close] invocation,
     * and you don't care about events, which not extracted by [extractAccumulatedValueOrNull] function at closing moment
     * @see [isClosed]
     * @see [receiveReducedEvents]
     * */
    fun receiveReducedEventsUntilClosed(receiver: (AccumulatedEventT) -> Unit) =
            receiveReducedEvents(this::isClosed, receiver)

    /**
     * Use it if you want to proceed all events, passed through [publishEvent] function before [close] invocation,
     * including events, which not extracted by [extractAccumulatedValueOrNull] function at closing moment
     * @see [isClosedAndEmpty]
     * @see [receiveReducedEvents]
     * */
    fun receiveReducedEventsUntilClosedAndEmpty(receiver: (AccumulatedEventT) -> Unit) =
            receiveReducedEvents(this::isClosedAndEmpty, receiver)

    /**
     * Uses current thread. Extracts accumulated events by [extractAccumulatedValueOrNull] function
     * and pass non-null of them in given [receiver], until [stopCondition] became true
     * */
    fun receiveReducedEvents(stopCondition: () -> Boolean, receiver: (AccumulatedEventT) -> Unit) {
        while (!stopCondition.invoke()) {
            val eventOrNull = extractAccumulatedValueOrNull()
            if (eventOrNull != null) {
                receiver.invoke(eventOrNull)
            }
        }
    }

    companion object {
        @JvmStatic
        fun <EventT> lastEventWinAccumulator(
                extractTimeoutMs: Long = DEFAULT_EXTRACT_TIMEOUT_MS
        ) = ReducingEventAccumulator(
                reduceFunction = { _: EventT?, event: EventT -> event },
                extractTimeoutMs = extractTimeoutMs
        )
    }
}