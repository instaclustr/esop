package com.instaclustr.esop.impl.hash;

import java.util.List;

import com.instaclustr.esop.impl.ManifestEntry;


/**
 * Service for hashing sstable components in parallel.
 */
public interface ParallelHashService extends HashService, AutoCloseable {

    /**
     * Hashes and populates the hash field of the provided manifest entries in parallel. Blocks until operation is completed or failed.
     */
    void hashAndPopulate(List<ManifestEntry> manifestEntries);

    /**
     * Verifies all manifest entries in parallel, if verification of any ManifestEntry fails it stops left submitted verifications.
     * Blocks until operation is completed or failed.
     */
    void verifyAll(final List<ManifestEntry> manifestEntries) throws HashVerificationException;
}
