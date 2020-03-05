package ru.fix.stdlib.concurrency.futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * maxPendingCount - максимальное число pending futures которые могут быть зарегистрированы в limiter
 *
 * @author Kamil Asfandiyarov
 */
public class PendingFutureLimiter {
    private final Logger log = LoggerFactory.getLogger(PendingFutureLimiter.class);

    private final AtomicLong counter = new AtomicLong();
    private final ConcurrentHashMap<CompletableFuture, Long> pendingCompletableFutures = new ConcurrentHashMap<>();
    private volatile int maxPendingCount;
    private volatile float thresholdFactor = 0.3f;
    private volatile long maxFutureExecuteTime;
    private volatile long waitTimeToCheckSizeQueue = TimeUnit.MINUTES.toMillis(1);

    private ThresholdListener thresholdListener;

    /**
     * @param maxPendingCount - максимальное количество фитч запущенных паралельно
     * @deprecated используйте {@link #PendingFutureLimiter(int, long)} вместо этого контруктора,
     * лучше вручную устанавливать значение {@link #maxFutureExecuteTime} в 15 минут
     */
    @Deprecated
    public PendingFutureLimiter(int maxPendingCount) {
        this(maxPendingCount, TimeUnit.MINUTES.toMillis(15));
    }

    /**
     * @param maxPendingCount         максимальное количество {@link CompletableFuture} запущенных паралельно
     * @param maxFutureExecuteTimeout максимальное время (в миллисекундах) на выполнение {@link CompletableFuture},
     *                                при достижении данного времени {@link CompletableFuture} будет принудительно
     *                                завершена с помощью {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    public PendingFutureLimiter(int maxPendingCount, long maxFutureExecuteTimeout) {
        this.maxPendingCount = maxPendingCount;
        this.maxFutureExecuteTime = maxFutureExecuteTimeout;
    }

    /**
     * @param maxPendingCount         максимальное количество {@link CompletableFuture} запущенных паралельно
     * @param maxFutureExecuteTimeout максимальное время (в миллисекундах) на выполнение {@link CompletableFuture},
     *                                при достижении данного времени {@link CompletableFuture} будет принудительно
     *                                завершена с помощью {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    public PendingFutureLimiter(DynamicProperty<Integer> maxPendingCount, DynamicProperty<Long> maxFutureExecuteTimeout) {
        this.maxPendingCount = maxPendingCount.get();
        this.maxFutureExecuteTime = maxFutureExecuteTimeout.get();

        maxPendingCount.addListener((oldValue, newValue) -> setMaxPendingCount(newValue));
        maxFutureExecuteTimeout.addListener((oldValue, newValue) -> setMaxFutureExecuteTime(newValue));
    }

    private static int calculateThreshold(int maxPendingCount, float thresholdFactor) {
        return (int) (maxPendingCount * (1 - thresholdFactor));
    }

    protected int getThreshold() {
        return calculateThreshold(maxPendingCount, thresholdFactor);
    }

    protected int getMaxPendingCount() {
        return maxPendingCount;
    }

    /**
     * Изменяет максимальное число pending futures
     */
    public PendingFutureLimiter setMaxPendingCount(int maxPendingCount) {

        int threshold = calculateThreshold(maxPendingCount, thresholdFactor);

        if (threshold <= 0 || threshold >= maxPendingCount) {
            throw new IllegalArgumentException("Invalid thresholdFactor");
        }

        this.maxPendingCount = maxPendingCount;

        synchronized (counter) {
            counter.notifyAll();
        }

        return this;
    }

    public float getThresholdFactor() {
        return thresholdFactor;
    }

    public void setMaxFutureExecuteTime(long maxFutureExecuteTime) {
        this.maxFutureExecuteTime = maxFutureExecuteTime;
    }

    public void setTresspassingThresholdListener(ThresholdListener thresholdListener) {
        this.thresholdListener = thresholdListener;
    }

    /**
     * Устанавливает новое значение для thresholdFactor.
     * <p/>
     * ThresholdFactor задается как отношени свободных слотов к maxPendingCount,
     * при достижении которого будет подан сигнал о спаде критической нагрузки
     * в {@link ThresholdListener#onLimitSubceed()}.
     * Например, чтобы получить threshold 800 при maxPendingCount 1000,
     * нужно задать thresholdFactor = 0.2
     *
     * @param thresholdFactor % свободных слотов от maxPendingCount,
     *                        при достижении которого будет подан сигнал о спаде критической нагрузки
     *                        в {@link ThresholdListener#onLimitSubceed()}
     */
    public PendingFutureLimiter changeThresholdFactor(float thresholdFactor) {

        int threashold = calculateThreshold(maxPendingCount, thresholdFactor);

        if (threashold <= 0 || threashold >= maxPendingCount) {
            throw new IllegalArgumentException("Invalid thresholdFactor");
        }

        this.thresholdFactor = thresholdFactor;

        synchronized (counter) {
            counter.notifyAll();
        }

        return this;
    }

    /**
     * Неограниченное добавление future в очередь
     */
    public <T> CompletableFuture<T> enqueueUnlimited(CompletableFuture<T> future) throws InterruptedException {
        return internalEnqueue(future, false);
    }

    /**
     * Ограниченное добавление future в очередь.
     * Если очередь уже достигла maxPendingCount то блокирует вызвавший поток до освобождения очереди,
     * Далее, если maxFutureExecuteTime > 0, то по достижении этого времени выполнения в милисекундах,
     *      future удаляется из очереди и создаётся возможность для добавления в неё новых future
     * Если maxFutureExecuteTime = 0, до очередь не освобождается по таймауту и поставляющий в
     * очередь поток будет заблокирован, пока future в очереди не выполнятся
     */
    public <T> CompletableFuture<T> enqueueBlocking(CompletableFuture<T> future) throws InterruptedException {
        return internalEnqueue(future, true);
    }

    /**
     * Регистрирует future во внутреннем счетчике.
     * Подписывается на future listener который декрементит счетчик в случае завершения future.
     * блокируется - если текущее значение внутреннего счетчика достигает или превышает maxPendingCount
     * разблокируется - если количество pending futures = maxPendingCount * thresholdFactor
     * разблокирование происходит в методе запущеном в ForkJoinPool после того как future будет завершена
     * При наличии thresholdListener вызывает его методы в следующих случаях:
     * ThresholdListener::onLimitReached - перед блокировкой
     * ThresholdListener::onLimitSubceed - перед разблокировкой
     */
    protected <T> CompletableFuture<T> internalEnqueue(CompletableFuture<T> future,
                                                       boolean needToBlock) throws InterruptedException {
        if (counter.get() == maxPendingCount && thresholdListener != null) {
            thresholdListener.onLimitReached();
        }

        if (needToBlock) {
            awaitAndPurge();
        }

        counter.incrementAndGet();
        pendingCompletableFutures.put(future, System.currentTimeMillis());
        future.handleAsync((any, exc) -> {
            if (exc != null) {
                log.error(exc.getMessage(), exc);
            }
            long value = counter.decrementAndGet();
            pendingCompletableFutures.remove(future);

            if (value == 0 || value == getThreshold()) {
                if (thresholdListener != null) {
                    thresholdListener.onLimitSubceed();
                }
                synchronized (counter) {
                    counter.notifyAll();
                }
            }
            return null;
        });
        return future;
    }

    private void awaitAndPurge() throws InterruptedException {
        synchronized (counter) {
            while (counter.get() >= maxPendingCount && maxFutureExecuteTime > 0) {
                counter.wait(waitTimeToCheckSizeQueue);
                releaseEnqueued();
            }
        }
    }

    public long getPendingCount() {
        return counter.get();
    }

    /**
     * Освобождение очереди от всех выполняющихся future
     * WARNING!! Исполняющиеся future не будут прерваны или завершены, они будут просто удалены из очереди!
     */
    public void releaseAll() throws InterruptedException {
        synchronized (counter) {
            while (counter.get() > 0) {
                counter.wait(waitTimeToCheckSizeQueue);
                releaseEnqueued();
            }
        }
    }

    private void releaseEnqueued() {
        String errorMessage = "Timeout exception. Completable future did not complete for at least "
                + maxFutureExecuteTime + " milliseconds. Pending count: " + getPendingCount();

        Consumer<Map.Entry<CompletableFuture, Long>> completeExceptionally =
                entry -> entry.getKey().completeExceptionally(new Exception(errorMessage));

        Predicate<Map.Entry<CompletableFuture, Long>> isTimeoutPredicate =
                entry -> System.currentTimeMillis() - entry.getValue() > maxFutureExecuteTime;

        log.error(errorMessage);

        pendingCompletableFutures.entrySet().stream().filter(isTimeoutPredicate).forEach(completeExceptionally);
    }

    public interface ThresholdListener {
        /**
         * вызывается если текущее значение внутреннего счетчика достигает maxPendingCount
         * и выполняется в том же потоке в котором был вызван текущий метод.
         */
        void onLimitReached();

        /**
         * вызывается если количество pending futures = maxPendingCount * thresholdFactor
         * выполняется в отдельном потоке
         */
        void onLimitSubceed();
    }
}
