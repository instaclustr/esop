package com.instaclustr.cassandra.backup.local;

import com.instaclustr.cassandra.backup.impl.BucketService;

public class LocalBucketService extends BucketService {

    @Override
    public boolean doesExist(final String bucketName) {
        return true;
    }

    @Override
    public void createIfMissing(final String bucketName) {

    }

    @Override
    public void create(final String bucketName) {

    }

    @Override
    public void delete(final String bucketName) {

    }

    @Override
    public void close() {

    }
}
