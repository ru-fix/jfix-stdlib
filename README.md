# jfix-stdlib

Provides common functionality that enhance usability of standard jvm.

# jfix-stdlib-concurrency

[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-stdlib-concurrency.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

Named executors helps to monitor Threads state, tasks latency and throughput.  
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