package com.instaclustr.esop.impl.retry;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Retrier {

    void submit(final Runnable r) throws Exception;

    <T> T submit(final Callable<T> c) throws Exception;

    class DefaultRetrier implements Retrier {

        private final Logger logger = LoggerFactory.getLogger(DefaultRetrier.class);

        private int attempts = 0;
        private final int maxAttempts;
        protected final RetrySpec retrySpec;
        protected final Sleeper sleeper;

        public DefaultRetrier(final RetrySpec retrySpec, final Sleeper sleeper) {
            this.retrySpec = retrySpec;
            this.sleeper = sleeper;
            this.maxAttempts = retrySpec.maxAttempts;
        }

        @Override
        public <T> T submit(final Callable<T> c) throws Exception {
            reset();

            T result = null;

            while (attempts <= maxAttempts) {
                try {
                    result = c.call();
                    break;
                } catch (final Exception ex) {
                    if (ex instanceof RetriableException) {
                        attempts += 1;
                        if (attempts > maxAttempts) {
                            throw ex;
                        }
                        logger.error("This operation will be retried: " + ex.getMessage(), ex);
                        sleeper.sleep();
                    } else {
                        throw ex;
                    }
                }
            }

            return result;
        }

        @Override
        public void submit(final Runnable r) {
            reset();
            while (attempts <= maxAttempts) {
                try {
                    r.run();
                    break;
                } catch (final Exception ex) {
                    if (ex instanceof RetriableException) {
                        attempts += 1;
                        if (attempts > maxAttempts) {
                            throw ex;
                        }
                        logger.error("This operation will be retried: " + ex.getMessage(), ex);
                        sleeper.sleep();
                    } else {
                        throw ex;
                    }
                }
            }
        }

        private void reset() {
            attempts = 0;
            sleeper.reset();
        }
    }

    class ExponentialSleeper implements Sleeper {

        private int originalInterval;
        private int interval;

        public ExponentialSleeper(final int interval) {
            this.interval = interval;
            this.originalInterval = interval;
        }

        @Override
        public void sleep() {
            Uninterruptibles.sleepUninterruptibly(interval, SECONDS);
            interval *= 2;
        }

        @Override
        public void reset() {
            interval = originalInterval;
        }
    }

    class LinearSleeper implements Sleeper {

        private final int interval;

        public LinearSleeper(final int interval) {
            this.interval = interval;
        }

        @Override
        public void sleep() {
            Uninterruptibles.sleepUninterruptibly(interval, SECONDS);
        }
    }

    interface Sleeper {

        void sleep();

        default void reset() {
        }
    }

    class RetriableException extends RuntimeException {

        public RetriableException(final String message) {
            super(message);
        }

        public RetriableException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
