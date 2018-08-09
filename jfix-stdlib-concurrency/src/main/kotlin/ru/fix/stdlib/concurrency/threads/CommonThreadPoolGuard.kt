package ru.fix.stdlib.concurrency.threads

import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.ForkJoinPool


/**
 * Regularly common pool size
 * Calls @listener when thread pool size outgrow queueThreshold
 */
open class CommonThreadPoolGuard(profiler: Profiler,
                                 checkRate: DynamicProperty<Schedule>,
                                 queueThreshold: DynamicProperty<Int>,
                                 listener: (queueSize: Int, threadDump: String) -> Unit
) : ForkJoinThreadPoolGuard(profiler, checkRate, ForkJoinPool.commonPool(), queueThreshold, listener)