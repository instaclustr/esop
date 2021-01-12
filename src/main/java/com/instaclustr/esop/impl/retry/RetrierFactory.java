package com.instaclustr.esop.impl.retry;

import static com.instaclustr.esop.impl.retry.RetrySpec.RetryStrategy.EXPONENTIAL;
import static com.instaclustr.esop.impl.retry.RetrySpec.RetryStrategy.LINEAR;
import static java.lang.String.format;

import java.util.concurrent.Callable;

import com.instaclustr.esop.impl.retry.Retrier.DefaultRetrier;
import com.instaclustr.esop.impl.retry.Retrier.ExponentialSleeper;
import com.instaclustr.esop.impl.retry.Retrier.LinearSleeper;

public class RetrierFactory {

    private static final class NoOpRetrier implements Retrier {

        @Override
        public void submit(final Runnable r) {
            r.run();
        }

        @Override
        public <T> T submit(final Callable<T> c) throws Exception {
            return c.call();
        }
    }

    public synchronized static Retrier getRetrier(final RetrySpec retrySpec) {
        if (retrySpec == null || retrySpec.strategy == null) {
            return new NoOpRetrier();
        }
        if (retrySpec.strategy == LINEAR) {
            return new DefaultRetrier(retrySpec, new LinearSleeper(retrySpec.interval));
        } else if (retrySpec.strategy == EXPONENTIAL) {
            return new DefaultRetrier(retrySpec, new ExponentialSleeper(retrySpec.interval));
        } else {
            throw new IllegalStateException(format("Unable to construct a retrier of startegy %s", retrySpec.strategy));
        }
    }
}
