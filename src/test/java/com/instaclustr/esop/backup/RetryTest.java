package com.instaclustr.esop.backup;

import com.instaclustr.esop.impl.retry.Retrier;
import com.instaclustr.esop.impl.retry.RetrierFactory;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.esop.impl.retry.RetrySpec.RetryStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RetryTest {

    @Test
    public void exponentialRetryTest() throws Exception {

        RetrySpec retrySpec = new RetrySpec();
        retrySpec.enabled = true;
        retrySpec.strategy = RetryStrategy.EXPONENTIAL;
        retrySpec.maxAttempts = 3;
        retrySpec.interval = 5;

        Retrier retrier = RetrierFactory.getRetrier(retrySpec);

        long start = System.currentTimeMillis();

        try {
            retrier.submit(() -> {
                System.out.println(System.currentTimeMillis() + " ... running");
                throw new Retrier.RetriableException("I have to retry!");
            });

            Assert.fail("should fail!");
        } catch (final Exception ex) {
            long duration = System.currentTimeMillis() - start;
            System.out.println(duration);
        }
    }

    @Test
    public void linearRetryTest() throws Exception {

        RetrySpec retrySpec = new RetrySpec();
        retrySpec.enabled = true;
        retrySpec.strategy = RetryStrategy.LINEAR;
        retrySpec.maxAttempts = 3;
        retrySpec.interval = 5;

        Retrier retrier = RetrierFactory.getRetrier(retrySpec);

        long start = System.currentTimeMillis();

        try {
            retrier.submit(() -> {
                System.out.println(System.currentTimeMillis() + " ... running");
                throw new Retrier.RetriableException("I have to retry!");
            });

            Assert.fail("should fail!");
        } catch (final Exception ex) {
            long duration = System.currentTimeMillis() - start;
            System.out.println(duration);
            Assert.assertTrue(duration >= ((retrySpec.maxAttempts - 1) * retrySpec.interval) * 1000);
        }
    }
}
