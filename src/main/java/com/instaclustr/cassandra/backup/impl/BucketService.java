package com.instaclustr.cassandra.backup.impl;

public interface BucketService extends AutoCloseable {

    boolean doesExist(String bucketName);

    default void createIfMissing(String bucketName) {
        if (!doesExist(bucketName)) {
            create(bucketName);
        }
    }

    void create(String bucketName);

    void delete(String bucketName);
}
