package ru.fix.stdlib.concurrency.threads

import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.management.ManagementFactory
import java.util.concurrent.ForkJoinPool

open class ThreadPoolGuard(profiler: Profiler,
                           checkRate: DynamicProperty<Schedule>,
                           private val predicate: () -> Boolean,
                           private val listener: (queueSize: Int, threadDump: String) -> Unit) : AutoCloseable {

    private val scheduler = NamedExecutors.newScheduler(
            "thread-pool-guard",
            DynamicProperty.of(1),
            profiler)

    init {
        scheduler.schedule(checkRate, 0, Runnable {
            val queueSize = ForkJoinPool.commonPool().queuedSubmissionCount

            if (predicate()) {
                listener(queueSize, buildDump())
            }
        })
    }

    private fun buildDump(): String {
        val dump = StringBuilder()
        val threadMXBean = ManagementFactory.getThreadMXBean()
        val threadInfos = threadMXBean.getThreadInfo(threadMXBean.allThreadIds, 1000)
        for (threadInfo in threadInfos) {
            dump.append("\"")
            dump.append(threadInfo.threadName)
            dump.append("\" ")
            val state = threadInfo.threadState
            dump.append(state)
            val stackTraceElements = threadInfo.stackTrace
            for (stackTraceElement in stackTraceElements) {
                dump.append("\n    at ")
                dump.append(stackTraceElement)
            }
            dump.append("\n")
        }
        return dump.toString()
    }

    override fun close() {
        scheduler.shutdown()
    }
}