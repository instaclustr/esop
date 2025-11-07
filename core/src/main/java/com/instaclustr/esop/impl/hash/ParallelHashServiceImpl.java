package com.instaclustr.esop.impl.hash;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

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

    @Override
    public ForkJoinTask<?> hashAndPopulate(final List<ManifestEntry> manifestEntries) {
        logger.info("Starting parallel hashing of manifest entries using {} threads.", forkJoinPool.getParallelism());
        return forkJoinPool.submit(() -> manifestEntries.parallelStream().forEach(entry -> entry.hash = hash(entry)));
    }

    public ForkJoinTask<?> verifyAll(final List<ManifestEntry> manifestEntries, OnFailure onFailure) {
        logger.info("Starting parallel verification of manifest entries using {} threads.", forkJoinPool.getParallelism());
        return forkJoinPool.submit(() -> manifestEntries.parallelStream().forEach(entry -> {
            try {
                verify(entry);
            } catch (Exception e) {
                onFailure.accept(entry, e);
            }
        }));
    }

    @Override
    public void close() {
        forkJoinPool.shutdown();
    }
}
