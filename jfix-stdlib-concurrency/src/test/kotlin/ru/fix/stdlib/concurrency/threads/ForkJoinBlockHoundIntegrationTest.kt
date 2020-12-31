package ru.fix.stdlib.concurrency.threads

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KLogging
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance
import reactor.blockhound.BlockHound
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ForkJoinPool

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class ForkJoinBlockHoundIntegrationTest {

    @BeforeAll
    fun setUp() {
        BlockHound.install(ForkJoinBlockHoundIntegration())
    }

    @TestFactory
    fun `WHEN some blocking operation called in fork join pool through CompletableFuture THEN error thrown`() =
        DynamicTest.stream(
            TestCase.CASES.iterator(),
            {
                it.description
            }
        ) { (_, blockingCall) ->
            CompletableFuture.supplyAsync {
                blockingCall()
            }.join()
        }

    @TestFactory
    fun `WHEN some blocking operation called in fork join pool through coroutines THEN error thrown`() =
        DynamicTest.stream(
            TestCase.CASES.iterator(),
            {
                it.description
            }
        ) { (_, blockingCall) ->
            `WHEN some blocking operation called in fork join pool through coroutines THEN error thrown`(blockingCall)
        }

    private fun `WHEN some blocking operation called in fork join pool through coroutines THEN error thrown`(
        blockingCall: () -> Unit
    ) = runBlocking {
        withContext(ForkJoinPool.commonPool().asCoroutineDispatcher()) {
            blockingCall()
        }
    }

    companion object: KLogging() {

        data class TestCase(val description: String, val blockingCall: () -> Unit) {
            companion object {
                val CASES = listOf<TestCase>(
                    //TestCase("Thread.sleep") { Thread.sleep(1_000) },
                    //TestCase("synchronized function") { ClassWithSynchronizedFunction().synchronizedFunction() }
                    //TestCase("synchronized kotlin function") { ClassWithSynchronizedFunction().kotlinSynchronizedFunction() }
                    //TestCase("blocking join() call") { CompletableFuture.runAsync { }.join() }
                TestCase("lol") {}
                )
            }
        }

        class ClassWithSynchronizedFunction {

            @Synchronized
            fun synchronizedFunction() {
            }

            fun kotlinSynchronizedFunction() = synchronized(this) {}
        }
    }

}