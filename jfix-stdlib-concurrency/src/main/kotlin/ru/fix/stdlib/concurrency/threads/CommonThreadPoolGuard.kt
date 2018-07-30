package ru.fix.stdlib.concurrency.threads

import ru.fix.commons.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.ForkJoinPool


open class CommonThreadPoolGuard(profiler: Profiler,
                            checkRate: DynamicProperty<Schedule>,
                            queueThreshold: DynamicProperty<Int>,
                             listener: (queueSize: Int, threadDump: String) -> Unit
) : ForkJoinThreadPoolGuard(profiler, checkRate, ForkJoinPool.commonPool(), queueThreshold, listener)