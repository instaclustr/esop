package com.instaclustr.scheduling;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;
import com.instaclustr.operations.OperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DaemonScheduler<R extends OperationRequest, T extends Operation<R>> {

    private static final Logger logger = LoggerFactory.getLogger(DaemonScheduler.class);

    private final Time rate;
    private boolean isSetup = false;
    private boolean isShutdown = false;
    private final ScheduledExecutorService executorService;
    private final Supplier<T> operationSupplier;

    private ScheduledFuture<?> scheduledFuture;

    public DaemonScheduler(final Time rate, final Supplier<T> operationSupplier) {
        if (rate == null) {
            throw new IllegalStateException("Rate of execution can not be null!");
        }
        this.rate = rate;
        this.executorService = Executors.newScheduledThreadPool(1);
        this.operationSupplier = operationSupplier;
    }

    public void setup() {
        if (isSetup) {
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutDown));

        isSetup = true;
    }

    public void execute() {
        setup();

        scheduledFuture = executorService.scheduleAtFixedRate(() -> {
            final String nextExecution = Instant.now().plusMillis(rate.asSeconds().toMilliseconds()).toString();
            T operation = operationSupplier.get();

            operation.run();

            if (operation.hasErrors()) {
                for (final Error e : operation.errors) {
                    logger.info(e.toString());
                }
            }

            logger.info("Operation will be next executed at " + nextExecution);
        }, 0, rate.value, TimeUnit.valueOf(rate.unit.name()));

        final Thread t = new Thread(() -> {
            try {
                while (!scheduledFuture.isDone() && !scheduledFuture.isCancelled()) {
                    Thread.sleep(5000);
                }
            } catch (final Exception ex) {
                logger.error("Exception thrown while sleeping", ex);
            }
        });

        t.start();

        try {
            t.join();
        } catch (final Exception ex) {
            logger.error("Error while waiting for thread to join.", ex);
        }
    }

    public void shutDown() {
        logger.info("Shutting down scheduler");
        if (!isSetup) {
            logger.info("Scheduler was not set up, nothing to shut down");
            return;
        }

        if (isShutdown) {
            logger.info("Scheduler was already shut down");
            return;
        }

        if (!scheduledFuture.isDone() && !scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(false);
        }

        executorService.shutdown();

        try {
            logger.info("Awaiting termination of running tasks");
            executorService.awaitTermination(1, TimeUnit.HOURS);
            logger.info("Done.");
        } catch (InterruptedException e) {
            logger.error("Exception occurred while waiting for termination of executor service", e);
        } finally {
            isShutdown = true;
        }
    }
}
