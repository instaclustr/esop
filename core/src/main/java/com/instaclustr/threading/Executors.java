package com.instaclustr.threading;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class Executors {

    public static final Integer DEFAULT_CONCURRENT_CONNECTIONS = 10;

    public static final class FixedTasksExecutorSupplier extends ExecutorServiceSupplier {

        @Override
        public ListeningExecutorService get(final Integer concurrentTasks) {
            if (concurrentTasks != null) {
                return MoreExecutors.listeningDecorator(java.util.concurrent.Executors.newFixedThreadPool(concurrentTasks));
            } else {
                return MoreExecutors.listeningDecorator(java.util.concurrent.Executors.newFixedThreadPool(DEFAULT_CONCURRENT_CONNECTIONS));
            }
        }
    }

    public static abstract class ExecutorServiceSupplier {

        public ListeningExecutorService get() {
            return get(DEFAULT_CONCURRENT_CONNECTIONS);
        }

        public abstract ListeningExecutorService get(final Integer concurrentTasks);
    }
}
