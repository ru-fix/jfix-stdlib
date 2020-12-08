# jfix-stdlib

Provides common functionality that enhance usability of standard jvm.

* [jfix-stdlib-concurrency](#jfix-stdlib-concurrency)
* [jfix-stdlib-ratelimiter](#jfix-stdlib-ratelimiter)
* [jfix-stdlib-socket](#jfix-stdlib-socket)

# jfix-stdlib-concurrency

[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-stdlib-concurrency.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

## NamedExecutors DynamicPool
Named executors helps to monitor threads state, tasks latency and throughput.  
All pools can be dynamically reconfigured  
```kotlin

@PropertyId("dao.thread.pool.size")
lateinit var daoPoolSize: DynamicProperty<Integer>
lateinit var profiler: Profiler
...

val executor = NamedExecutors.newDynamicPool(
                "dao-pool",
                daoPoolSize,
                profiler)
```

## NamedExecutors Scheduler
Scheduler is based on `ScheduledThreadPoolExecutor` but it can change it's rate dynamically.
```kotlin

@PropertyId("work.rate")
lateinit var rate: DynamicProperty<Long>
@PropertyId("pool.size")
lateinit var poolSize: DynamicProperty<Int>

lateinit var profiler: Profiler
...

val scheduler = NamedExecutors.newScheduler(
        "regular-work-pool",
        poolSize,
        profiler)         
        
scheduler.schedule(
        Schedule.withRate(rate),
        0,//delay
        Runnable {
            //do work here
        })        
               
```

## NamedExecutors metrics
Common metrics that will work out of the box:

* `pool.<poolName>.queue` - size of pending tasks submitted to pool
* `pool.<poolName>.activeThreads` - count of currently running threads
* `pool.<poolName>.await` - how many ms spent task in pending state before pool took task for execution
* `pool.<poolName>.run` - how many ms task executed
* `pool.<poolName>.poolSize` - current size of the pool


Special case is Common Fork Join Pool that uses different set of metrics:
```kotlin
lateinit var profiler: Profiler
...
//Enable Common Fork Join Pool profiling
NamedExecutors.profileCommonPool(profiler)
```

* `pool.commonPool.poolSize` - current size of the pool
* `pool.commonPool.activeThread` - count of threads in the pool
* `pool.commonPool.runningThread` - count of currently active not blocking threads
* `pool.commonPool.queue` - size of pending tasks submitted to pool
* `pool.commonPool.steal` - count of stolen tasks
 
![](docs/pool-metric.png?raw=true)

## ThreadPoolGuard
CommonThreadPoolGuard, ThreadPoolGuard allows you to watch for queue size of the thread pool. 
If it outgrows threshold guard will invoke user handler and print stack trace of all threads.

```kotlin
val guard = CommonThreadPoolGuard(
                profiler,
                checkRate,
                threshold) { queueSize, dump ->
            log.error("Queue size $queueSize is too big. Current threads state: $dump")
        }
```
 

# jfix-stdlib-ratelimiter

[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-stdlib-ratelimiter.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

Provides RateLimiter and RateLimitedDispatcher.

## RateLimiter

`RateLimiter` interface is defined here as abstraction for `RateLimitedDispatcher`. 
You can wrap whatever rate limiter implementation you want into it.

Out of the box there is one implementation - `ConfigurableRateLimiter`. Under the hood it uses `AtomicRateLimiter` 
of [resilience4j-ratelimiter](https://github.com/resilience4j/resilience4j) preconfigured it to acquire limits smoothly. 
AtomicRateLimiter does not distribute events inside refresh period, it releases all available permits immediately on 
interval start if there is a demand for them. To distribute event in our interval of 1 second `ConfigurableRateLimiter` 
divides it into chunks so that 1 chunk of time is limited to 1 permit.The drawback is that actual rate will be lower 
than configured - depending on requests' distribution. 

## RateLimitedDispatcher

Enables async usage of rate limiter. Submitted tasks are executed in the order of submission.

Provides following metrics:

* `RateLimiterDispatcher.<dispatcherName>.queue_size` – incoming tasks queue size
* `RateLimiterDispatcher.<dispatcherName>.queue_wait` – task's wait time in the queue before execution
* `RateLimiterDispatcher.<dispatcherName>.acquire_limit` - time to acquire limit
* `RateLimiterDispatcher.<dispatcherName>.supplied_operation` - supplied task execution duration

# jfix-stdlib-socket

[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-stdlib-socket.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

Provides SocketChecker.

## SocketChecker

Allows to check port availability by its port number. Allows to get random free port number in range from 30000 to 60000.

## JMH
Hot to run jmh tests for id generator: 
```
cd jfix-stdlib-id-generator-jmh
gradle --stop
gradle --no-daemon clean build jmh
```

# jfix-stdlib-batching

Encapsulates batching execution of several client's operations as one task.

Follow example of batch sending of http requests to different hosts:

```kotlin
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.stdlib.batching.BatchTask
import ru.fix.stdlib.batching.BatchingManager
import ru.fix.stdlib.batching.BatchingParameters
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import kotlin.streams.asSequence

data class SimpleHttpBatchingConfig(
    val httpClient: HttpClient
)

data class SimpleBatchedHttpPayload(
    val batchItemContent: Int
) {
    val resultFuture: CompletableFuture<String> = CompletableFuture()
}

data class SimpleHttpBatchKeyType(val httpUri: URI)

object SimpleHttpBatchTask : BatchTask<SimpleHttpBatchingConfig, SimpleBatchedHttpPayload, SimpleHttpBatchKeyType> {

    override fun process(
        config: SimpleHttpBatchingConfig,
        batch: MutableList<SimpleBatchedHttpPayload>,
        key: SimpleHttpBatchKeyType
    ) {
        // merge content from several requests into one batched
        val joinedContent =
            batch.joinToString(separator = ", ", prefix = "[", postfix = "]") { it.batchItemContent.toString() }

        // send batched http request
        config.httpClient.sendAsync(
            HttpRequest.newBuilder()
                .uri(key.httpUri)
                .POST(HttpRequest.BodyPublishers.ofString(joinedContent))
                .build(),
            HttpResponse.BodyHandlers.ofLines()
        ).thenApplyAsync {
            // let client code know that some response arrived
            for ((index, response) in it.body().asSequence().withIndex()) {
                batch[index].resultFuture.complete(response)
            }
        }
        // do not block and process to the next batch
        // HOWEVER it is up to client to decide whether to block or not
    }

}

val batchingManagerProfiler = AggregatingProfiler()

val batchingManager = BatchingManager<SimpleHttpBatchingConfig, SimpleBatchedHttpPayload, SimpleHttpBatchKeyType>(
    SimpleHttpBatchingConfig(
        httpClient = HttpClient.newBuilder()
            .executor(Executors.newSingleThreadExecutor())
            .build()
    ),
    SimpleHttpBatchTask,
    BatchingParameters().apply {
        batchSize = 256
        batchThreads = 2
    },
    "simple-batch-manager",
    batchingManagerProfiler
)


// Usage in clients
val testHost1 = SimpleHttpBatchKeyType(httpUri = URI.create("http://test-host-1"))
val testHost2 = SimpleHttpBatchKeyType(httpUri = URI.create("http://test-host-2"))

val testHost1Requests = (1..3).map {
    SimpleBatchedHttpPayload(batchItemContent = it)
}

val testHost2Requests = (2..7).map {
    SimpleBatchedHttpPayload(batchItemContent = it)
}

for (testHost1Request in testHost1Requests) {
    batchingManager.enqueue(testHost1, testHost1Request)
}
for (testHost2Request in testHost2Requests) {
    batchingManager.enqueue(testHost2, testHost2Request)
}

(testHost1Requests + testHost2Requests).forEach { request ->
    request.resultFuture.thenApplyAsync { response ->
        println("""Request with content='${request.batchItemContent} - response is '$response''""")
    }
}
```