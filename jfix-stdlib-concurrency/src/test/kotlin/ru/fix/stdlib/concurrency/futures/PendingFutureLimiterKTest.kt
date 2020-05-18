package ru.fix.stdlib.concurrency.futures

import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val internalTimeoutMs = TimeUnit.MINUTES.toMillis(1)
private const val CHECKS_QUANTITY = 100

internal class PendingFutureLimiterKTest {


    private val limiter = PendingFutureLimiter(
            DynamicProperty.of(2),
            DynamicProperty.of(internalTimeoutMs)
    )


    @Test
    fun `waitAll with timeout WHEN enqueue completed future AND breaks exist THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                Thread.sleep(10)
                limiter.waitAll(internalTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue 2 completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll(internalTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll(internalTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll without timeout WHEN enqueue completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll()
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue not completed future THEN limiter doesn't stuck`() {
        val lock = ReentrantLock()
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                lock.lock()
                limiter.enqueueBlocking(CompletableFuture.runAsync { lock.withLock {} })
                lock.unlock() //ensure future wasn't completed before being queued
                limiter.waitAll(internalTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll without timeout WHEN enqueue not completed future THEN limiter doesn't stuck`() {
        val lock = ReentrantLock()
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                lock.lock()
                limiter.enqueueBlocking(CompletableFuture.runAsync { lock.withLock {} })
                lock.unlock() //ensure future wasn't completed before being queued
                limiter.waitAll()
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue unlimited completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(Duration.ofSeconds(10), { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueUnlimited(CompletableFuture.completedFuture(null))
                limiter.waitAll()
            }
        }
    }

    companion object {
        val log = KotlinLogging.logger { }
    }
}