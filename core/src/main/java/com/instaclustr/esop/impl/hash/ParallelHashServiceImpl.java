package com.instaclustr.esop.impl.hash;

import com.instaclustr.esop.impl.ManifestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of ParallelHashService that uses ForkJoinPool for parallel hashing and verification.
 * Implements AutoCloseable to ensure proper shutdown of the ForkJoinPool.
 */
public class ParallelHashServiceImpl extends HashServiceImpl implements ParallelHashService, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ParallelHashServiceImpl.class);

    private final ExecutorService executorService;
    private final int parallelism;

    public ParallelHashServiceImpl(final HashSpec hashSpec, final int parallelHashingThreads) {
        super(hashSpec);
        executorService = Executors.newFixedThreadPool(parallelHashingThreads);
        this.parallelism = parallelHashingThreads;
    }

    @Override
    public void hashAndPopulate(final List<ManifestEntry> manifestEntries) {
        logger.info("Starting parallel hashing of manifest entries using {} threads.", parallelism);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (final ManifestEntry entry : manifestEntries) {
                futures.add(CompletableFuture.runAsync(() -> entry.hash = hash(entry), executorService));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            throw new HashingException("Hashing failed for one or more manifest entries.", e);
        }
    }

    @Override
    public void verifyAll(List<ManifestEntry> manifestEntries) throws HashVerificationException {
        logger.info("Starting parallel verification of manifest entries using {} threads.", parallelism);

        List<Runnable> runnables = new ArrayList<>();
        for (final ManifestEntry manifestEntry : manifestEntries) {
            runnables.add(() ->
                          {
                              try {
                                  verify(manifestEntry);
                              } catch (Exception e) {
                                  logger.error("Hash verification failed, reason:", e);
                                  throw e;
                              }
                          });
        }

        verifyAllRunnables(runnables);
    }

    @Override
    public void verifyAllRunnables(List<Runnable> runnables) throws HashVerificationException
    {
        logger.info("Starting parallel verification of manifest entries using {} threads.", parallelism);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Runnable runnable : runnables) {
            futures.add(CompletableFuture.runAsync(runnable, executorService));
        }

        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((r, ex) -> {
                if (ex != null) {
                    futures.forEach(f -> f.cancel(true));
                }
            });
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException | CancellationException e) {
            throw new HashVerificationException("Hash verification failed for one or more manifest entries.");
        }
    }

    @Override
    public void close() {
        try {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                logger.warn(String.format("Executor service of %s was not terminated in a timely manner.", getClass().getName()));
            }
        }
        catch (Throwable t) {
            logger.warn(String.format("Exception occurred while shutting down executor service of %s", getClass().getName()));
        }
    }
}
