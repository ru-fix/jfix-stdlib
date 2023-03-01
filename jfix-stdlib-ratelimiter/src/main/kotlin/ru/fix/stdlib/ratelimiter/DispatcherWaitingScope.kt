package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.*
import mu.KotlinLogging
import org.slf4j.LoggerFactory
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.ForkJoinPool
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

open class DispatcherWaitingScope(
        private val waitTimeout: DynamicProperty<Long>,
        context: CoroutineContext = DispatcherCommonPoolScope.coroutineContext
) : CoroutineScope {

    companion object {
        val log = KotlinLogging.logger { }
    }

    private val parentJob = SupervisorJob()

    override val coroutineContext: CoroutineContext = context + parentJob

    /**
     * Ожидает завершения работы корутин, но не больше, чем [waitTimeout]
     *
     * Вернет true, если все корутины контекста завершили свою работу
     * иначе вернет false
     */
    fun waitChildrenAndCancel(): Boolean {
        val waitingResult = runBlocking {
            try {
                withTimeout(waitTimeout.get()) {
                    parentJob.children.forEach { c -> c.join() }
                }
            } catch (e: TimeoutCancellationException) {
                log.warn(e) {
                    "Scope couldn't complete execution for $waitTimeout ms. All coroutines will be " +
                            "canceled. Actual pending operations count: ${getPendingOperations()}"
                }
                parentJob.children.forEach { c -> c.cancel() }
                return@runBlocking false
            }
            return@runBlocking true
        }

        parentJob.cancel()
        return waitingResult
    }

    fun getPendingOperations(): Int {
        return parentJob.children.count()
    }

    private object DispatcherCommonPoolScope : CoroutineScope {
        private val log = LoggerFactory.getLogger(DispatcherCommonPoolScope::class.java)

        override val coroutineContext = EmptyCoroutineContext +
                ForkJoinPool.commonPool().asCoroutineDispatcher() +
                CoroutineExceptionHandler { context, thr ->
                    log.error(context.toString(), thr)
                } +
                CoroutineName("CommonPool")
    }

}