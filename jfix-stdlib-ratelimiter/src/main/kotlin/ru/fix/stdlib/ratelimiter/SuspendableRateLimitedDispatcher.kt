package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.PrefixedProfiler
import ru.fix.aggregating.profiler.ProfiledCall
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.dynamic.property.api.PropertySubscription
import java.lang.invoke.MethodHandles
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.log

/**
 * Manages tasks execution with given rate and window.
 * Rate specify how many requests per second will be dispatched.
 * Window size specify how many async operations with uncompleted result allowed.
 * When Window or Rate restriction is reached, dispatcher will stop to process requests and enqueue them in umbound queue.
 * Dispatcher executes all operations in single dedicated thread.
 *
 * @param name           name of dispatcher - will be used in metrics and worker's thread name
 * @param rateLimiter    rate limiter, which provides rate of operation
 * @param windowSize     ho many async operations with uncompleted result are allowed in dispatcher
 * @param closingTimeout max amount of time (in milliseconds) to wait for pending operations during shutdown.
 *                       If parameter equals 0 then dispatcher will not wait pending operations during closing process.
 *                       Any negative number will be interpreted as 0.
 *
 */
class SuspendableRateLimitedDispatcher(
    name: String,
    rateLimiter: RateLimiter,
    profiler: Profiler,
    windowSize: DynamicProperty<Int>,
    closingTimeout: DynamicProperty<Long>,
    executorService: ExecutorService? = null,
    coroutineScope: CoroutineScope = CommonPoolScope
) : AutoCloseable {

    companion object {
        private const val QUEUE_SIZE_INDICATOR = "queue_size"
        private const val ACTIVE_ASYNC_OPERATIONS = "active_async_operations"
    }

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private val state = AtomicReference<State>()

    private val rateLimiter: RateLimiter
    private val deferreds = CopyOnWriteArrayList<Deferred<Any?>>()

    private val name: String
    private val profiler: Profiler

    /**
     * Accessed only by single dedicated thread
     */
    private var windowSize = 0
    private val closingTimeout: DynamicProperty<Long>

    private val windowSemaphore: Semaphore
    private val windowSizeSubscription: PropertySubscription<Int>

    /**
     * How many async operations executed but their results are not ready yet.
     */
    private val activeAsyncOperations = AtomicInteger()
    private val queueSize = AtomicInteger()

    /**
     * Creates new dispatcher instance
     */
    init {
        this.name = name
        this.rateLimiter = rateLimiter
        this.closingTimeout = closingTimeout
        windowSemaphore = Semaphore(0)

        this.profiler = PrefixedProfiler(profiler, "RateLimiterDispatcher.$name.")

        state.set(State.RUNNING)

        logger.info("Attached indicators")
        this.profiler.attachIndicator(QUEUE_SIZE_INDICATOR) { queueSize.get().toLong() }
        this.profiler.attachIndicator(ACTIVE_ASYNC_OPERATIONS) { activeAsyncOperations.get().toLong() }

        windowSizeSubscription = windowSize.createSubscription().setAndCallListener { oldValue: Int?, newValue: Int ->
            ChangeWindowSizeCommand(oldValue ?: 0, newValue).apply()
        }
    }

    private fun asyncOperationStarted() {
        activeAsyncOperations.incrementAndGet()
        logger.info("Started, operations: ${activeAsyncOperations.get()}")
    }

    private fun asyncOperationCompleted() {
        activeAsyncOperations.decrementAndGet()
        windowSemaphore.release()
        logger.info("Completed, operations: ${activeAsyncOperations.get()}, permits ${windowSemaphore.availablePermits()}")
    }

    suspend fun <T> compose(supplier: () -> T): Deferred<T> {
        return submit {
            val res = supplier.invoke()
            asyncOperationCompleted()
            res
        }
    }

    suspend fun <AsyncResultT> compose(
            asyncOperation: () -> AsyncResultT,
            asyncResultSubscriber: (asyncResult: AsyncResultT, asyncResultCallback: () -> Unit) -> Unit
    ): Deferred<AsyncResultT> {
        return submit {
            val asyncResult = asyncOperation.invoke()
            asyncResultSubscriber.invoke(asyncResult) { asyncOperationCompleted() }
            asyncResult
        }
    }

    /**
     * Submits new operation to task queue.
     *
     *
     * WARNING: task should not be long running operation
     * and should not block processing thread.
     *
     * @param supplier task to execute and retrieve result
     * @return feature which represent result of task execution
     */
    private suspend fun <T> submit(supplier: () -> T): Deferred<T> {
        logger.info("submit1 in thread {}", Thread.currentThread())
        val state = state.get()
        return if (state != State.RUNNING) {
            throw RejectedExecutionException(
                    "RateLimiterDispatcher [$name] is in '$state' state"
            )
        } else {
            val queueWaitTime = profiler.start("queue_wait")
            queueSize.incrementAndGet()
            // TODO semaphor
            // TODO Scope
            val deferred = CommonPoolScope.async {
                processTask(Task2(supplier, queueWaitTime))
            }
            deferreds.add(deferred)
            deferred.invokeOnCompletion { deferreds.remove(deferred) }
            deferred
        }
    }

    fun updateRate(rate: Int) {
        rateLimiter.updateRate(rate)
    }

    override fun close() {
        var stateUpdated = state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN)
        if (!stateUpdated) {
            logger.info("Close called on RateLimitedDispatcher [{}] with state [{}]", name, state.get())
            return
        }
        windowSizeSubscription.close()

        deferreds.forEach {
            it.cancel("Lol")    //TODO message
        }

        if (closingTimeout.get() < 0) {
            logger.warn(
                "Rate limiter timeout must be greater than or equals 0. Current value is {}, rate limiter name: {}",
                closingTimeout.get(), name
            )
        }
        stateUpdated = state.compareAndSet(State.SHUTTING_DOWN, State.TERMINATE)
        if (!stateUpdated) {
            logger.error(
                "Can't set [TERMINATE] state to RateLimitedDispatcher [{}] in [{}] state",
                name, state.get()
            )
            return
        }
        rateLimiter.close()
        logger.info("Detached indicators")
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR)
        profiler.detachIndicator(ACTIVE_ASYNC_OPERATIONS)
    }


    private suspend fun <T> processTask(task: Task2<T>): T {
        logger.info("processTask in thread {}", Thread.currentThread())
        task.queueWaitTimeCall.stop()
        queueSize.decrementAndGet()
        try {
            if (windowSize > 0) {
                profiler.start("acquire_window").use { acquireWindowTime ->
                    var windowAcquired = false
                    while (!windowAcquired) {
                        if (state.get() == State.TERMINATE) {
                            logger.error("LOL1")    // TODO message
                            throw RejectedExecutionException("RateLimitedDispatcher [$name] TODO message 1")
                        }
                        windowAcquired = windowSemaphore.tryAcquire(500, TimeUnit.MILLISECONDS)
                        logger.info("windowAcquired {}", windowAcquired)
                        if (!windowAcquired) {
                            delay(2500) // TODO to const val
                        }
                    }
                    logger.info("Window acquired, permits ${windowSemaphore.availablePermits()}")
                    acquireWindowTime.stop()
                }
            }
            profiler.start("acquire_limit").use { limitAcquireTime ->
                var limitAcquired = false
                while (!limitAcquired) {
                    if (state.get() == State.TERMINATE) {
                        logger.error("LOL2")    // TODO message
                        throw RejectedExecutionException("RateLimitedDispatcher [$name] TODO message 2")
                    }
                    limitAcquired = rateLimiter.tryAcquire(3, ChronoUnit.SECONDS)
                }
                limitAcquireTime.stop()
            }

            // Since async operation may complete faster then Started method call
            // it must be called before asynchronous operation started
            asyncOperationStarted()
            val result: T = profiler.profile(
                    "supply_operation",
                    Supplier { task.supplier.invoke() }
            )!!

            return result
        } catch (e: Exception) {
            // TODO message
            logger.error("Error message: $e")
            throw RejectedExecutionException("RateLimitedDispatcher [$name] TODO message")
        }
    }

    private open class Task2<T>(
            val supplier: () -> T,
            val queueWaitTimeCall: ProfiledCall
    )

    private interface Command {
        fun apply()
    }

    private inner class ChangeWindowSizeCommand(private val oldSize: Int, private val newSize: Int) :
        Command {
        override fun apply() {
            if (newSize == oldSize) return
            windowSize = newSize
            if (newSize > oldSize) {
                windowSemaphore.release(newSize - oldSize)
            } else {
                windowSemaphore.acquire(oldSize - newSize)
            }
        }
    }

    private enum class State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }

    object CommonPoolScope : CoroutineScope {
        private val log = LoggerFactory.getLogger(CommonPoolScope::class.java)

        override val coroutineContext = EmptyCoroutineContext +
                ForkJoinPool.commonPool().asCoroutineDispatcher() +
                CoroutineExceptionHandler { context, thr ->
                    log.error(context.toString(), thr)
                } +
                CoroutineName("CommonPool")
    }

}