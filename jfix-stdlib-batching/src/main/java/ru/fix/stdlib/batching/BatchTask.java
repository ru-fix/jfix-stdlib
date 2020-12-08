package ru.fix.stdlib.batching;

import java.util.List;

/**
 * @author Kamil Asfandiyarov
 */
@FunctionalInterface
public interface BatchTask<ConfigT, PayloadT, KeyT> {
    void process(ConfigT config, List<PayloadT> batch, KeyT key) throws InterruptedException;
}
