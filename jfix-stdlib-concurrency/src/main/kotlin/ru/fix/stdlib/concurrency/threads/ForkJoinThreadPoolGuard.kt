package ru.fix.stdlib.concurrency.threads

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.ForkJoinPool


open class ForkJoinThreadPoolGuard(profiler: Profiler,
                                   checkRate: DynamicProperty<Schedule>,
                                   pool: ForkJoinPool,
                                   queueThreshold: DynamicProperty<Int>,
                                   listener: (queueSize: Int, threadDump: String) -> Unit
) : ThreadPoolGuard(profiler,
        checkRate,
        { pool.queuedSubmissionCount > queueThreshold.get() },
        listener
)