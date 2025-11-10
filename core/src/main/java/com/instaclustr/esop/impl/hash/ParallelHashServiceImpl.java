package com.instaclustr.esop.impl.hash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import com.google.common.annotations.VisibleForTesting;
import com.instaclustr.esop.impl.ManifestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public ForkJoinTask<?> hashAndPopulate(final List<ManifestEntry> manifestEntries) {
        logger.info("Starting parallel hashing of manifest entries using {} threads.", forkJoinPool.getParallelism());
        return forkJoinPool.submit(() -> manifestEntries.parallelStream().forEach(entry -> entry.hash = hash(entry)));
    }

    public void verifyAll(final List<ManifestEntry> manifestEntries) throws HashVerificationException {
        logger.info("Starting parallel verification of manifest entries using {} threads.", forkJoinPool.getParallelism());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (final ManifestEntry manifestEntry : manifestEntries) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> verify(manifestEntry), forkJoinPool);
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .exceptionally(ex -> {
                        for (CompletableFuture<Void> future : futures) {
                            future.cancel(true);
                        }
                        throw new HashVerificationException("Error during parallel verification of manifest entries.", ex);
                    })
                    .join();
        } catch (Exception e) {
            throw e.getCause() instanceof HashVerificationException
                    ? (HashVerificationException) e.getCause()
                    : new HashVerificationException("Error during parallel verification of manifest entries.", e);
        }
    }

    //     public void verifyAll(final List<ManifestEntry> manifestEntries, OnFailure onFailure) {
    //        logger.info("Starting parallel verification of manifest entries using {} threads.", forkJoinPool.getParallelism());
    //        try {
    //            forkJoinPool.submit(() -> manifestEntries.parallelStream()
    //                    .forEach(entry -> {
    //                        try {
    //                            verify(entry);
    //                        } catch (Exception e) {
    //                            forkJoinPool.shutdownNow();
    //                            throw e;
    //                        }
    //                    }))
    //                    .join();
    //        } catch (Exception e) {
    //            throw new HashVerificationException("Error during parallel verification of manifest entries.", e);
    //        }
    //    }

    @Override
    public void close() {
        forkJoinPool.shutdown();
    }
}
