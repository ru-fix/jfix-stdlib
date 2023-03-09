package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import org.slf4j.LoggerFactory
import java.util.concurrent.ForkJoinPool
import kotlin.coroutines.EmptyCoroutineContext

object TestCommonPoolScope : CoroutineScope {
    private val log = LoggerFactory.getLogger(TestCommonPoolScope::class.java)

    override val coroutineContext = EmptyCoroutineContext +
            ForkJoinPool.commonPool().asCoroutineDispatcher() +
            CoroutineExceptionHandler { context, thr ->
                log.error(context.toString(), thr)
            } +
            CoroutineName("CommonPool")
}