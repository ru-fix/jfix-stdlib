package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import mu.KotlinLogging
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.PrefixedProfiler
import ru.fix.aggregating.profiler.ProfiledCall
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.dynamic.property.api.PropertySubscription
import java.time.temporal.ChronoUnit
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

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
        private val name: String,
        private val rateLimiter: RateLimiter,
        profiler: Profiler,
        windowSize: DynamicProperty<Int>,
        closingTimeout: DynamicProperty<Long>,
        context: CoroutineContext = DispatcherCommonPoolScope.coroutineContext
) : AutoCloseable {

    companion object {
        private const val QUEUE_SIZE_INDICATOR = "queue_size"
        private const val ACTIVE_ASYNC_OPERATIONS = "active_async_operations"
        private const val WINDOW_ACQUIRE_DELAY_MS = 3_000L

        private val logger = KotlinLogging.logger {}
    }

    private val state = AtomicReference<State>()

    private val deferreds = CopyOnWriteArrayList<Deferred<Any?>>()

    private val profiler: Profiler

    /**
     * Accessed only by single dedicated thread
     */
    private var windowSize = 1

    private val windowSizeSubscription: PropertySubscription<Int>

    /**
     * How many async operations executed but their results are not ready yet.
     */
    private val activeAsyncOperations = AtomicInteger()
    private val queueSize = AtomicInteger()

    private val semaphore: AtomicReference<Semaphore> = AtomicReference(Semaphore(1))

    private val waitingScope: DispatcherWaitingScope = DispatcherWaitingScope(closingTimeout, context)

    /**
     * Creates new dispatcher instance
     */
    init {
        this.profiler = PrefixedProfiler(profiler, "RateLimiterDispatcher.$name.")

        state.set(State.RUNNING)

        this.profiler.attachIndicator(QUEUE_SIZE_INDICATOR) { queueSize.get().toLong() }
        this.profiler.attachIndicator(ACTIVE_ASYNC_OPERATIONS) { activeAsyncOperations.get().toLong() }

        windowSizeSubscription = windowSize.createSubscription().setAndCallListener { oldValue: Int?, newValue: Int ->
            runBlocking {
                ChangeWindowSizeCommand(oldValue ?: 0, newValue).apply()
            }
        }
    }

    private fun asyncOperationStarted() {
        activeAsyncOperations.incrementAndGet()
    }

    private fun asyncOperationCompleted() {
        activeAsyncOperations.decrementAndGet()
        semaphore.get().release()
    }

    suspend fun <T> compose(supplier: () -> T): Deferred<T> {
        return submit {
            val res = supplier.invoke()
            asyncOperationCompleted()
            res
        }
    }

//    suspend fun <AsyncResultT> compose(
//            asyncOperation: () -> AsyncResultT,
//            asyncResultSubscriber: (asyncResult: AsyncResultT, asyncResultCallback: () -> Unit) -> Unit
//    ): Deferred<AsyncResultT> {
//        return submit {
//            val asyncResult = asyncOperation.invoke()
//            asyncResultSubscriber.invoke(asyncResult) { asyncOperationCompleted() }
//            asyncResult
//        }
//    }

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
        val state = state.get()
        return if (state != State.RUNNING) {
            throw RejectedExecutionException(
                    "RateLimiterDispatcher [$name] is in '$state' state"
            )
        } else {
            val queueWaitTime = profiler.start("queue_wait")
            queueSize.incrementAndGet()

            val deferred = waitingScope.async {
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
            it.cancel("Cancelling deferred on dispatcher close")
        }

        if (!waitingScope.waitChildrenAndCancel()) {
            logger.warn("Timeout while waiting for coroutines finishing")
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
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR)
        profiler.detachIndicator(ACTIVE_ASYNC_OPERATIONS)
    }


    private suspend fun <T> processTask(task: Task2<T>): T {
        task.queueWaitTimeCall.stop()
        try {
            if (windowSize > 0) {
                profiler.start("acquire_window").use { acquireWindowTime ->
                    var windowAcquired = false
                    while (!windowAcquired) {
                        if (state.get() == State.TERMINATE) {
                            val message = "RateLimitedDispatcher [$name] received TERMINATE state while window acquiring"
                            logger.error(message)
                            throw RejectedExecutionException(message)
                        }
                        windowAcquired = withTimeoutOrNull(WINDOW_ACQUIRE_DELAY_MS) {
                            semaphore.get().acquire()
                            true
                        } ?: false
                    }
                    acquireWindowTime.stop()
                }
            }
            profiler.start("acquire_limit").use { limitAcquireTime ->
                var limitAcquired = false
                while (!limitAcquired) {
                    if (state.get() == State.TERMINATE) {
                        val message = "RateLimitedDispatcher [$name] received TERMINATE state while rate limiter acquiring"
                        logger.error(message)
                        throw RejectedExecutionException(message)
                    }
                    limitAcquired = rateLimiter.tryAcquire(3, ChronoUnit.SECONDS)
                }
                limitAcquireTime.stop()
            }

            queueSize.decrementAndGet()
            // Since async operation may complete faster then Started method call
            // it must be called before asynchronous operation started
            asyncOperationStarted()

            return profiler.profile(
                    "supply_operation",
                    Supplier { task.supplier.invoke() }
            )!!
        } catch (e: Exception) {
            logger.error("RateLimitedDispatcher [$name] received exception: $e")
            throw RejectedExecutionException("RateLimitedDispatcher [$name] received exception: ${e.message}")
        }
    }

    private open class Task2<T>(
            val supplier: () -> T,
            val queueWaitTimeCall: ProfiledCall
    )

    private interface Command {
        suspend fun apply()
    }

    private inner class ChangeWindowSizeCommand(private val oldSize: Int, private val newSize: Int) :
        Command {
        override suspend fun apply() {
            if (newSize == oldSize) return
            semaphore.getAndUpdate {
                val acquirePermits = windowSize - it.availablePermits
                windowSize = newSize
                Semaphore(newSize, acquirePermits)
            }
        }
    }

    private enum class State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }

    private object DispatcherCommonPoolScope : CoroutineScope {
        private val log = LoggerFactory.getLogger(DispatcherCommonPoolScope::class.java)

        override val coroutineContext = EmptyCoroutineContext +
                ForkJoinPool.commonPool().asCoroutineDispatcher() +
                CoroutineExceptionHandler { context, thr ->
                    log.error(context.toString(), thr)
                } +
                CoroutineName("CommonPool")
    }

}