package com.instaclustr.cassandra.backup.guice;

import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public interface BucketServiceFactory<T extends BucketService> {

    T createBucketService(final BackupOperationRequest request);

    T createBucketService(final BackupCommitLogsOperationRequest request);
}
