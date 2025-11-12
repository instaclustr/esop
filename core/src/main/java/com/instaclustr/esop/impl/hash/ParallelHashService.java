package com.instaclustr.esop.impl.hash;

import com.instaclustr.esop.impl.ManifestEntry;

import java.util.List;


/**
 * Service for hashing sstable components in parallel.
 */
public interface ParallelHashService extends HashService, AutoCloseable
{

    /**
     * Hashes and populates the hash field of the provided manifest entries in parallel. Blocks until operation is completed or failed.
     */
    void hashAndPopulate(List<ManifestEntry> manifestEntries);

    /**
     * Verifies all manifest entries in parallel, if verification of any ManifestEntry fails it stops left submitted verifications.
     * Blocks until operation is completed or failed.
     */
    void verifyAll(List<ManifestEntry> manifestEntries) throws HashVerificationException;

    void verifyAllRunnables(List<Runnable> runnables) throws HashVerificationException;
}
