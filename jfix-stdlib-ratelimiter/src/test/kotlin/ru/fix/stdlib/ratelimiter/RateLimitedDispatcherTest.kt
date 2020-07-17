package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class RateLimitedDispatcherTest {
    private companion object {
        private val logger = LoggerFactory.getLogger(RateLimitedDispatcherTest::class.java)
    }

    @Test
    fun `dispatch async operation with user defined async result type, operation invoked and it's result returned`() {
        class UserAsyncResult() {
            fun whenComplete(callback: () -> Unit) {
            }
        }

        val dispatcher = createDispatcher()

        val asyncResultInstance = UserAsyncResult()

        fun userAsyncOperation(): UserAsyncResult {
            return asyncResultInstance
        }

        val delayedSubmission = dispatcher.compose(
                { userAsyncOperation() },
                { asyncResult, callback -> asyncResult.whenComplete { callback.onAsyncResultCompleted() } }
        )

        (asyncResultInstance === delayedSubmission.get()).shouldBeTrue()

        dispatcher.close()
    }

    @Test
    fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`() {
        val dispatcher = createDispatcher()

        val operationResult = Object()
        fun userAsyncOperation(): CompletableFuture<Any> {
            return completedFuture(operationResult)
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        (operationResult === delayedSubmissionFuture.get()).shouldBeTrue()

        dispatcher.close()
    }


    @Test
    fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() {
        val dispatcher = createDispatcher()

        val asyncOperationException = Exception("some error")

        fun userAsyncOperation(): CompletableFuture<Any> {
            return CompletableFuture<Any>().apply {
                completeExceptionally(asyncOperationException)
            }
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        await().atMost(Duration.ofSeconds(10)).until {
            delayedSubmissionFuture.isCompletedExceptionally
        }

        val actualException = shouldThrow<Exception> { delayedSubmissionFuture.get() }

        actualException.cause.shouldNotBeNull()
        actualException.cause.shouldBe(asyncOperationException)

        dispatcher.close()
    }

    @Test
    fun `if windows size is 0, then restricted only by limiter `() {
        `async operations are restricted by limiter limit`(0)
    }

    @Test
    fun `if window size is not empty and quite big, restricted by limiter`() {
        `async operations are restricted by limiter limit`(100_000)
    }


    private fun `async operations are restricted by limiter limit`(windowSize: Int) {

        val RATE_REQ_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_REQ_PER_SECOND

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = RATE_REQ_PER_SECOND,
                window = windowSize
        )

        val counter = AtomicInteger(0)

        val profiler = AggregatingProfiler()
        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val features = List(ITERATIONS) {
            dispatcher.compose {
                profiledCall.profile<CompletableFuture<Int>> {
                    completedFuture(counter.incrementAndGet())
                }
            }
        }

        logger.info("Submit $ITERATIONS operations.")
        features.forEach { it.join() }

        counter.get().shouldBe(ITERATIONS)

        val report = profilerReporter.buildReportAndReset().profilerCallReports[0]

        features.map { it.join() }.toSet().containsAll((1..ITERATIONS).toList())

        logger.info("Throughput " + report.stopThroughputAvg)

        report.stopThroughputAvg.shouldBeBetween(
                RATE_REQ_PER_SECOND.toDouble(),
                RATE_REQ_PER_SECOND.toDouble(),
                RATE_REQ_PER_SECOND.toDouble() * 0.25)


        dispatcher.close()
    }


    @Test
    fun `window blocks number of uncompleted operations `() {

        val dispatcher = createDispatcher(window = 10)

        val futures = List(10) { CompletableFuture<Int>() }
        for (future in futures) {
            dispatcher.compose {
                future
            }
        }

        val asyncOperationIsInvoked = AtomicBoolean(false)

        val asyncOperationSubmissionResult = dispatcher.compose {
            asyncOperationIsInvoked.set(true)
            completedFuture(11)
        }

        sleep(3000)
        asyncOperationSubmissionResult.isDone.shouldBeFalse()

        futures[5].complete(5)

        await().atMost(Duration.ofSeconds(10)).until {
            asyncOperationSubmissionResult.isDone
        }
        asyncOperationSubmissionResult.get().shouldBe(11)

        futures.forEachIndexed { index, future -> future.complete(index) }

        dispatcher.close()
    }


    @Disabled("TODO")
    @Test
    fun `'queue_size' indicator shows unprocessed pending requests in queue`() {

    }

    @Disabled("TODO")
    @Test
    fun `'queue_wait' metric shows time span between enqueueing and dequeueing`() {

    }

    @Disabled("TODO")
    @Test
    fun `'acquire_limit' metric shows how long it took to pass rate limit restriction`() {

    }

    @Disabled("TODO")
    @Test
    fun `'acquire_window' metric shows how long it took to pass window size restriction`() {

    }


    @Disabled("TODO")
    @Test
    fun `metric shows queue-wait`() {

    }

    @Disabled("TODO")
    @Test
    fun `increasing window size allows to submit new operations until new limit is reached`() {
    }


    @Disabled("TODO")
    @Test
    fun `decreasing window size reduce limit`() {
    }


    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    fun `on shutdown fast tasks complete normally`() {

        val dispatch = createDispatcher(closingTimeout = 5_000)

        assertTimeoutPreemptively(Duration.ofSeconds(10)) {

            val blockingTaskIsStarted = CountDownLatch(1)


            dispatch.compose {
                blockingTaskIsStarted.countDown()
                //Due to blocking nature of dispatch.close we hae to use sleep
                Thread.sleep(1000)
                completedFuture(true)
            }

            val futures = List(3) {
                dispatch.compose {
                    completedFuture(true)
                }
            }

            blockingTaskIsStarted.await()
            dispatch.close()

            CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeFalse()
            }
        }
    }

    @Test
    fun `on shutdown slow tasks complete exceptionally`() {
        val dispatch = createDispatcher(closingTimeout = 0)

        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            val blockingTaskIsStarted = CountDownLatch(1)

            dispatch.compose {
                blockingTaskIsStarted.countDown()
                //Due to blocking nature of dispatch.close we hae to use sleep
                Thread.sleep(1000)
                completedFuture(true)
            }

            val futures = ArrayList<CompletableFuture<*>>()
            for (i in 1..3) {
                futures.add(dispatch.compose {
                    completedFuture(true)
                })
            }

            blockingTaskIsStarted.await()
            dispatch.close()

            CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeTrue()
                shouldThrow<ExecutionException> { future.get() }
                        .cause.shouldBeInstanceOf<RejectedExecutionException>()
            }
        }
    }

    private fun createDispatcher(
            rateLimitRequestPerSecond: Int = 500,
            window: Int = 0,
            closingTimeout: Int = 5000) =
            RateLimitedDispatcher(
                    "dispatcher-name",
                    ConfigurableRateLimiter("rate-limiter-name", rateLimitRequestPerSecond),
                    NoopProfiler(),
                    window,
                    DynamicProperty.of(closingTimeout.toLong())
            )

}
