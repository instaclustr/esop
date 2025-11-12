package com.instaclustr.esop.impl.hash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import com.google.common.annotations.VisibleForTesting;
import com.instaclustr.esop.impl.ManifestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ParallelHashService that uses ForkJoinPool for parallel hashing and verification.
 * Implements AutoCloseable to ensure proper shutdown of the ForkJoinPool.
 */
public class ParallelHashServiceImpl extends HashServiceImpl implements ParallelHashService, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ParallelHashServiceImpl.class);
    private final ForkJoinPool forkJoinPool;

    public ParallelHashServiceImpl(final HashSpec hashSpec, final int parallelHashingThreads) {
        super(hashSpec);
        forkJoinPool = new ForkJoinPool(parallelHashingThreads);
    }

    @VisibleForTesting
    // For testing purposes only
    public ParallelHashServiceImpl(final HashSpec hashSpec, ForkJoinPool forkJoinPool) {
        super(hashSpec);
        this.forkJoinPool = forkJoinPool;
    }

    @Override
    public void hashAndPopulate(final List<ManifestEntry> manifestEntries) {
        logger.info("Starting parallel hashing of manifest entries using {} threads.", forkJoinPool.getParallelism());
        try {
            forkJoinPool.submit(() -> manifestEntries.parallelStream().forEach(entry -> entry.hash = hash(entry))).get();
        } catch (Exception e) {
            throw new HashingException("Hashing failed for one or more manifest entries.", e);
        }
    }

    @Override
    public void verifyAll(final List<ManifestEntry> manifestEntries) throws HashVerificationException {
        logger.info("Starting parallel verification of manifest entries using {} threads.", forkJoinPool.getParallelism());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (final ManifestEntry manifestEntry : manifestEntries) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    verify(manifestEntry);
                } catch (Exception e) {
                    logger.error("Hash verification failed, reason:", e);
                    // Cancel all other futures if one fails. Hashing operations should be interruptable in case if we want to stop them prematurely.
                    cancelAll(futures);
                    throw e;
                }
            }, forkJoinPool);
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            throw new HashVerificationException("Hash verification failed for one or more manifest entries.", e);
        }
    }

    private static void cancelAll(List<CompletableFuture<Void>> futures) {
        for (CompletableFuture<Void> future : futures) {
            future.cancel(true);
        }
    }

    @Override
    public void close() {
        forkJoinPool.shutdown();
    }
}
