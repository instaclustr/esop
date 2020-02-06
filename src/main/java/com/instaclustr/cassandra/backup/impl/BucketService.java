package com.instaclustr.cassandra.backup.impl;

public interface BucketService extends AutoCloseable {

    boolean doesExist(String bucketName);

    default void createIfMissing(String bucketname) {
        if (!doesExist(bucketname)) {
            create(bucketname);
        }
    }

    void create(String bucketName);

    void delete(String bucketName);
}
