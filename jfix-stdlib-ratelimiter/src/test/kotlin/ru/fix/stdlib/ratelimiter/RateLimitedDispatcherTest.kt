package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.shouldBe
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.CompletableFuture
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
            return CompletableFuture.completedFuture(operationResult)
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
            return CompletableFuture<Any>().apply{
                completeExceptionally(asyncOperationException)
            }
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        delayedSubmissionFuture.isCompletedExceptionally.shouldBeTrue()
        val actualException = shouldThrow<Exception> { delayedSubmissionFuture.get() }

        actualException.shouldBe(asyncOperationException)

        dispatcher.close()
    }

    @Test
    fun `if windows size is 0, then restricted only by limiter `() {

        val RATE_REQ_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_REQ_PER_SECOND

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = RATE_REQ_PER_SECOND,
                window = 0
        )

        val counter = AtomicInteger(0)

        val profiler = AggregatingProfiler()
        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val features = List(ITERATIONS) {
            dispatcher.compose {
                profiledCall.profile<CompletableFuture<Int>> {
                    CompletableFuture.completedFuture(counter.incrementAndGet())
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
    fun `if window size is not empty and quite big, restricted by limiter`() {

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
            CompletableFuture.completedFuture(11)
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


//
//
//    @Test
//    fun testSubmitIncrementThroughput() {
//        createDispatcher(2000).use {
//            assertTimeoutPreemptively(Duration.ofSeconds(15)) {
//                testThroughput { call, counter -> submitIncrement(it, call, counter) }
//            }
//        }
//    }
//
//    @Test
//    fun testComposeIncrementThroughput() {
//        createDispatcher(2000).use {
//            assertTimeoutPreemptively(Duration.ofSeconds(15)) {
//                testThroughput { call, counter -> composeIncrement(it, call, counter) }
//            }
//        }
//    }
//
    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
//    @Test
//    fun `on shutdown fast tasks complete normally`() {
//
//        val dispatch = createDispatcher(closingTimeout = 5_000)
//
//            assertTimeoutPreemptively(Duration.ofSeconds(10)) {
//
//                val blockingTaskIsStarted = CountDownLatch(1)
//
//
//                dispatch.submit {
//                    blockingTaskIsStarted.countDown()
//                    //Due to blocking nature of dispatch.close we hae to use sleep
//                    Thread.sleep(1000)
//                }
//                val futures = List(3) {
//                    dispatch.submit { }
//                }
//
//                blockingTaskIsStarted.await()
//                dispatch.close()
//
//                CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()
//
//                futures.forEach { future: CompletableFuture<*> ->
//                    assertTrue(future.isDone)
//                    assertFalse(future.isCompletedExceptionally)
//                }
//            }
//        }
//    }

//    @Test
//    fun shutdown_tasksNotCompletedInTimeout_areCompletedExceptionally() {
//        createDispatcher(0).use { dispatch ->
//            assertTimeoutPreemptively(Duration.ofSeconds(5)) {
//                val blockingTaskIsStarted = CountDownLatch(1)
//
//                dispatch.submit {
//                    blockingTaskIsStarted.countDown()
//                    //Due to blocking nature of dispatch.close we hae to use sleep
//                    Thread.sleep(1000)
//                }
//
//                val futures = ArrayList<CompletableFuture<*>>()
//                for (i in 1..3) {
//                    futures.add(dispatch.submit { })
//                }
//
//                blockingTaskIsStarted.await()
//                dispatch.close()
//
//                CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()
//
//                futures.forEach { future: CompletableFuture<*> ->
//                    assertTrue(future.isDone)
//                    assertTrue(future.isCompletedExceptionally)
//                }
//            }
//        }
//    }

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

//
//
//    private fun testThroughput(biFunction: (ProfiledCall, AtomicInteger) -> CompletableFuture<Int>) {
//
//
//
//
//        val counter = AtomicInteger(0)
//        val profiler = AggregatingProfiler()
//        val profilerReporter = profiler.createReporter()
//
//        profilerReporter.buildReportAndReset()
//        val profiledCall = profiler.profiledCall(RateLimitedDispatcher::class.toString())
//
//        val features = List(ITERATIONS) {
//            biFunction.invoke(profiledCall, counter)
//        }
//
//        CompletableFuture.allOf(*features.toTypedArray()).join()
//        val report = profilerReporter.buildReportAndReset().profilerCallReports[0]
//
//        val results = features
//                .map { it.join() }
//
//        for (i in 0 until ITERATIONS) {
//            assertTrue(results.contains(i))
//        }
//
//        logger.info("Current throughput " + report.stopThroughputAvg)
//
//        assertThat(report.stopThroughputAvg, lessThanOrEqualTo((RATE_LIMIT * 1.25 * 1000.toDouble())))
//
//
//        assertEquals(ITERATIONS, counter.get())
//    }
//
//    private fun submitIncrement(dispatcher: RateLimitedDispatcher,
//                                call: ProfiledCall,
//                                counter: AtomicInteger): CompletableFuture<Int> {
//        return dispatcher.submit {
//            call.profile<Int> {
//                counter.getAndIncrement()
//            }
//        }
//    }
//
//    private fun composeIncrement(dispatcher: RateLimitedDispatcher,
//                                 call: ProfiledCall,
//                                 counter: AtomicInteger): CompletableFuture<Int> {
//        return dispatcher.compose {
//            call.profile<CompletableFuture<Int>> {
//                CompletableFuture.supplyAsync { counter.getAndIncrement() }
//            }
//        }
//    }


}
