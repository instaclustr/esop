package com.instaclustr.esop.impl.hash;

import java.util.concurrent.ForkJoinTask;
import java.util.stream.Stream;

import com.instaclustr.esop.impl.ManifestEntry;


/**
 * Service for hashing sstable components in parallel.
 */
public interface ParallelHashService extends HashService, AutoCloseable {

    /**
     * Hashes and populates the hash field of the provided manifest entries in parallel.
     * It makes manifestEntries stream parallel.
     *
     * @param manifestEntries Stream of manifest entries to hash.
     * @return A ForkJoinTask representing the parallel hashing operation.
     */
    ForkJoinTask<?> hashAndPopulate(Stream<ManifestEntry> manifestEntries);
}
