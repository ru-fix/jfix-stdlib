package ru.fix.stdlib.ratelimiter

import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.*
import mu.KLogging
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.*
import java.time.Duration
import java.util.concurrent.*
import kotlin.coroutines.CoroutineContext

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class RateLimitedDispatcherProviderTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val LIMIT_PER_SECOND = 500.0
        const val CLOSING_TIMEOUT = 5000
    }

    @Test
    fun `WHEN swithcing isSuspendableFlag THEN provider switches dispatcher realisation`() {
        val rateLimitedDispatcherProvider = createDispatcherProvider(
            rateLimitRequestPerSecond = LIMIT_PER_SECOND.toInt(),
            closingTimeout = CLOSING_TIMEOUT,
            profiler = NoopProfiler(),
            context = Dispatchers.Default,
            isSuspendableOnStart = true
        )

        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
        rateLimitedDispatcherProvider.switchDispatcherProvider(false)
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<RateLimitedDispatcher>()
        rateLimitedDispatcherProvider.switchDispatcherProvider(true)
        rateLimitedDispatcherProvider.provideDispatcher().shouldBeInstanceOf<SuspendableRateLimitedDispatcher>()
    }

    @Nested
    inner class SuspendableDispatcherCompletableFutureTests {

        private val suspendableDispatcherTests: SuspendableRateLimitedDispatcherTest =
            object : SuspendableRateLimitedDispatcherTest() {

                override fun createDispatcher(
                    rateLimitRequestPerSecond: Int,
                    closingTimeout: Int,
                    profiler: Profiler,
                    context: CoroutineContext
                ): SuspendableRateLimitedDispatcher {
                    val provider = createDispatcherProvider(
                        rateLimitRequestPerSecond = rateLimitRequestPerSecond,
                        closingTimeout = closingTimeout,
                        profiler = profiler,
                        context = context,
                        isSuspendableOnStart = false
                    )
                    provider.switchDispatcherProvider(true)
                    return provider.provideDispatcher() as SuspendableRateLimitedDispatcher
                }
            }

        @Test
        fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`()
        }

        @Test
        fun `series of operations with CompletableFuture`() {
            suspendableDispatcherTests.CompletableFutureTests().`series of operations with CompletableFuture`()
        }

        @Test
        fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution with CompletableFuture`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution with CompletableFuture`()
        }

        @Test
        fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`()
        }

        @Test
        fun `on shutdown fast tasks with CompletableFuture complete normally`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`on shutdown fast tasks with CompletableFuture complete normally`()
        }

        @Test
        fun `on shutdown slow tasks with CompletableFuture complete exceptionally`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`on shutdown slow tasks with CompletableFuture complete exceptionally`()
        }

        @Test
        fun `task with CompletableFuture, submitted in closed dispatcher, is rejected with exception`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`task with CompletableFuture, submitted in closed dispatcher, is rejected with exception`()
        }

        @Test
        fun `invoke suspend function wrapped to completable future`() {
            suspendableDispatcherTests.CompletableFutureTests()
                .`invoke suspend function wrapped to completable future`()
        }

    }

    @Nested
    inner class SuspendableDispatcherSuspendTests {

        private val suspendableDispatcherTests: SuspendableRateLimitedDispatcherTest =
            object : SuspendableRateLimitedDispatcherTest() {

                override fun createDispatcher(
                    rateLimitRequestPerSecond: Int,
                    closingTimeout: Int,
                    profiler: Profiler,
                    context: CoroutineContext
                ): SuspendableRateLimitedDispatcher {
                    val provider = createDispatcherProvider(
                        rateLimitRequestPerSecond = rateLimitRequestPerSecond,
                        closingTimeout = closingTimeout,
                        profiler = profiler,
                        context = context,
                        isSuspendableOnStart = false
                    )
                    provider.switchDispatcherProvider(true)
                    return provider.provideDispatcher() as SuspendableRateLimitedDispatcher
                }
            }

        @Test
        fun `dispatch suspend operation with exceptional result, operation invoked and it's result returned`() {
            suspendableDispatcherTests.SuspendTests()
                .`dispatch suspend operation with exceptional result, operation invoked and it's result returned`()
        }

        @Test
        fun `series of operations`() {
            suspendableDispatcherTests.SuspendTests().`series of operations`()
        }

        @Test
        fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution`() {
            suspendableDispatcherTests.SuspendTests()
                .`'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution`()
        }

        @Test
        fun `WHEN many fast tasks completed THEN 'active_async_operations' is 0`() {
            suspendableDispatcherTests.SuspendTests()
                .`WHEN many fast tasks completed THEN 'active_async_operations' is 0`()
        }

        @Test
        fun `on shutdown fast tasks complete normally`() {
            suspendableDispatcherTests.SuspendTests().`on shutdown fast tasks complete normally`()
        }

        @Test
        fun `on shutdown slow tasks complete exceptionally`() {
            suspendableDispatcherTests.SuspendTests().`on shutdown slow tasks complete exceptionally`()
        }

        @Test
        fun `task, submitted in closed dispatcher, is rejected with exception`() {
            suspendableDispatcherTests.SuspendTests()
                .`task, submitted in closed dispatcher, is rejected with exception`()
        }

        @Test
        fun `invoke suspend function`() {
            suspendableDispatcherTests.SuspendTests().`invoke suspend function`()
        }

    }

    @Nested
    inner class SimpleDispatcherCompletableFutureTests {

        private val dispatcherTests: RateLimitedDispatcherTest = object : RateLimitedDispatcherTest() {

            override fun createDispatcher(
                rateLimitRequestPerSecond: Int,
                window: DynamicProperty<Int>,
                closingTimeout: Int,
                profiler: Profiler
            ): RateLimitedDispatcher {
                val provider = createDispatcherProvider(
                    rateLimitRequestPerSecond,
                    closingTimeout,
                    profiler,
                    Dispatchers.Default,
                    window,
                    true
                )
                provider.switchDispatcherProvider(false)
                return provider.provideDispatcher() as RateLimitedDispatcher
            }
        }

        @Test
        fun `dispatch async operation with user defined async result type, operation invoked and it's result returned`() {
            dispatcherTests.`dispatch async operation with user defined async result type, operation invoked and it's result returned`()
        }

        @Test
        fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`() {
            dispatcherTests.`dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`()
        }

        @Test
        fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() {
            dispatcherTests.`dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`()
        }

        @Test
        fun `if windows size is 0, then restricted only by limiter`() {
            dispatcherTests.`if windows size is 0, then restricted only by limiter`()
        }

        @Test
        fun `if window size is not empty and quite big, restricted by limiter`() {
            dispatcherTests.`if window size is not empty and quite big, restricted by limiter`()
        }

        @Test
        fun `when window of uncompleted operations is full no new operation is dispatched`() {
            dispatcherTests.`when window of uncompleted operations is full no new operation is dispatched`()
        }

        @Test
        fun `'queue_wait', 'acquire_limit', 'acquire_window', 'supplied_operation', 'queue_size', 'active_async_operations' metrics gathered during execution`() {
            dispatcherTests.`'queue_wait', 'acquire_limit', 'acquire_window', 'supplied_operation', 'queue_size', 'active_async_operations' metrics gathered during execution`()
        }

        @Test
        fun `indicators 'queue_size' and 'active_async_operations' adjusted according to number of queued and active operations`() {
            dispatcherTests.`indicators 'queue_size' and 'active_async_operations' adjusted according to number of queued and active operations`()
        }

        @Test
        fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() {
            dispatcherTests.`WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`()
        }

        @Test
        fun `WHEN completed futures arrived THEN indicators are correctly adjusted`() {
            dispatcherTests.`WHEN completed futures arrived THEN indicators are correctly adjusted`()
        }

        @Test
        fun `increasing window size allows to submit new operations up to the new limit`() {
            dispatcherTests.`increasing window size allows to submit new operations up to the new limit`()
        }

        @Test
        fun `decreasing window size reduces limit`() {
            dispatcherTests.`decreasing window size reduces limit`()
        }

        @Test
        fun `on shutdown fast tasks complete normally`() {
            dispatcherTests.`on shutdown fast tasks complete normally`()
        }

        @Test
        fun `on shutdown slow tasks complete exceptionally`() {
            dispatcherTests.`on shutdown slow tasks complete exceptionally`()
        }

        @Test
        fun `task, submitted in closed dispatcher, is rejected with exception`() {
            dispatcherTests.`task, submitted in closed dispatcher, is rejected with exception`()
        }

        @Test
        fun `invoke suspend function wrapped to completable future`() {
            dispatcherTests.`invoke suspend function wrapped to completable future`()
        }
    }

    private fun createDispatcherProvider(
        rateLimitRequestPerSecond: Int,
        closingTimeout: Int,
        profiler: Profiler,
        context: CoroutineContext,
        window: DynamicProperty<Int> = DynamicProperty.of(0),
        isSuspendableOnStart: Boolean,
    ): RateLimitedDispatcherProvider {
        return RateLimitedDispatcherProviderImpl(
            rateLimiterFactory = RateLimiterTestFactory(),
            rateLimiterSettings = DynamicProperty.of(
                RateLimiterSettings(
                    limitPerSec = rateLimitRequestPerSecond.toDouble(),
                    closeTimeout = Duration.ofSeconds(closingTimeout.toLong()),
                    isSuspendable = isSuspendableOnStart
                )
            ),
            profiler = profiler,
            suspendableWaitingScopeContext = context,
            limiterId = DISPATCHER_NAME,
            window = window
        )
    }

    /**
     * Change provider's dispatcher flag
     */
    fun RateLimitedDispatcherProvider.switchDispatcherProvider(isSuspendable: Boolean) {
        this::class.java.declaredMethods
            .find {
                it.name == "updateDispatcher"
            }!!
            .apply { isAccessible = true }
            .invoke(this, isSuspendable)
    }

}