package com.instaclustr.cassandra.backup.impl;

import static java.lang.String.format;

public abstract class BucketService implements AutoCloseable {

    public abstract boolean doesExist(String bucketName) throws BucketServiceException;

    public void createIfMissing(String bucketName) throws BucketServiceException {
        if (!doesExist(bucketName)) {
            create(bucketName);
        }
    }

    public abstract void create(String bucketName) throws BucketServiceException;

    public abstract void delete(String bucketName) throws BucketServiceException;

    public static class BucketServiceException extends Exception {

        public BucketServiceException(final String message) {
            super(message);
        }

        public BucketServiceException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    public void checkBucket(final String bucket, final Boolean createMissingBucket) throws Exception {
        boolean bucketExists;

        bucketExists = doesExist(bucket);

        if (!bucketExists && !createMissingBucket) {
            throw new BucketServiceException(format("Bucket %s does not exist and createMissingBucket is false! Can not continue!", bucket));
        }

        if (!bucketExists) {
            createIfMissing(bucket);
        }
    }
}
