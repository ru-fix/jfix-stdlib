package ru.fix.stdlib.concurrency.threads

import reactor.blockhound.BlockHound
import reactor.blockhound.integration.BlockHoundIntegration
import java.util.concurrent.ForkJoinWorkerThread

class ForkJoinBlockHoundIntegration: BlockHoundIntegration {

    override fun applyTo(builder: BlockHound.Builder) {
        builder
            .addDynamicThreadPredicate {
                it is ForkJoinWorkerThread
            }
            .nonBlockingThreadPredicate { predicate ->
                predicate.or {
                    it is ForkJoinWorkerThread
                }
            }
    }
}