package ru.fix.stdlib.concurrency.threads

import io.kotest.assertions.throwables.shouldThrow
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import java.lang.IllegalStateException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier

class AsyncSchedulerTest {

    @Test
    fun `fixed rate`() {
        val schedule = AsyncScheduler("schuduler", NoopProfiler())
        val flag = AtomicBoolean()
        schedule.schedule(Supplier<CompletionStage<Any?>> {
            flag.set(true)
            CompletableFuture.completedFuture(null)
        })
        await().until { flag.get() == true }
    }

    @Test
    fun `fixed delay`() {
        val schedule = AsyncScheduler("schuduler", NoopProfiler())
        TODO()
    }

    @Test
    fun `closed scheduler does not receive new tasks`() {
        val schedule = AsyncScheduler("schuduler", NoopProfiler())
        schedule.close()
        shouldThrow<IllegalStateException> {
            schedule.schedule(Runnable {})
        }
    }

    @Test
    fun `closed scheduler stops launching tasks`() {
        TODO()
    }

    @Test
    fun `fixed rate long running tasks do not accumulate in scheduller`() {
        TODO()
    }

    @Test
    fun `start delay`() {
        TODO()
    }

    @Test
    fun `change task schedule`() {
        TODO()
    }

    @Test
    fun `cancelled task stops launching`() {
        TODO()
    }

    @Test
    fun `cancel and schedule task does not impact other tasks`() {
        TODO()
    }

    @Test
    fun `long running task does not block others`() {
        TODO()
    }

    @Test
    fun `if async operation is actually not async and takes too much time to execute log an error`() {
        TODO()
    }
}