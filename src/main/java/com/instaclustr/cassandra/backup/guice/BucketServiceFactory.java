package com.instaclustr.cassandra.backup.guice;

import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;

public interface BucketServiceFactory<T extends BucketService> {

    T createBucketService(final BackupOperationRequest request);

    T createBucketService(final BackupCommitLogsOperationRequest request);

    T createBucketService(final RestoreOperationRequest request);

    T createBucketService(final RestoreCommitLogsOperationRequest request);
}
