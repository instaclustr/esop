package com.instaclustr.esop.impl.hash;

import java.util.List;
import java.util.concurrent.ForkJoinTask;

import com.instaclustr.esop.impl.ManifestEntry;


/**
 * Service for hashing sstable components in parallel.
 */
public interface ParallelHashService extends HashService, AutoCloseable {

    /**
     * Hashes and populates the hash field of the provided manifest entries in parallel.
     */
    ForkJoinTask<?> hashAndPopulate(List<ManifestEntry> manifestEntries);

    /**
     * Verifies all manifest entries in parallel, invoking the provided onFailure callback for any failures.
     */
    ForkJoinTask<?> verifyAll(final List<ManifestEntry> manifestEntries, OnFailure onFailure);

    interface OnFailure {
        void accept(ManifestEntry entry, Throwable throwable);
    }
}
