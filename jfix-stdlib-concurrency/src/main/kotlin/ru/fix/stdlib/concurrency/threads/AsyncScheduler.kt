package ru.fix.stdlib.concurrency.threads

import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.PriorityBlockingQueue
import java.util.function.Supplier

class AsyncScheduler(private val name: String, private val profiler: Profiler) : AutoCloseable {
    companion object {
        private val logger = LoggerFactory.getLogger(AsyncScheduler::class.java)
    }

    private class Task(
            val launchTime: Long,
            val operation: Supplier<CompletionStage<Any?>>,
            val cancellation: CompletionStage<Void>)

    private val priorityQueue = PriorityBlockingQueue<Task>(1) { first, second ->
        first.launchTime.compareTo(second.launchTime)
    }

    //thread aliveness is an indicator whether scheduler was closed or not
    private val thread = Thread(this::executeLoop, "async-scheduler-$name")

    private fun executeLoop() {
        try {
            while (!Thread.currentThread().isInterrupted) {
                val task = priorityQueue.take()
                try {
                    val future = task.operation.get()
                    TODO()
                } catch (exc: Exception) {
                    logger.error("Failed to launch task in AsyncScheduler $name", exc)
                }
            }
        } catch (exc: InterruptedException) {
            //exit loop
        } catch (exc: Exception) {
            logger.error("Aborting async scheduler due to unexpected error", exc)
        }
    }

    fun schedule(task: Supplier<CompletionStage<Any?>>): CompletionStage<Void> {
        val cancellation = CompletableFuture<Void>()
        priorityQueue.add(Task(System.currentTimeMillis(), task, cancellation))
        //thread could die immediately after new task added to the queue
        if (!thread.isAlive) {
            priorityQueue.clear()
            throw IllegalStateException("AsyncScheduler $name already closed.")
        }
        return cancellation
    }

    override fun close() {
        thread.interrupt()
        thread.join()
        priorityQueue.clear()
    }
}