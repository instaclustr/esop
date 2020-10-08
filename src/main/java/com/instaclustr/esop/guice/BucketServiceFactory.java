package com.instaclustr.esop.guice;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;

public interface BucketServiceFactory<T extends BucketService> {

    T createBucketService(final BackupOperationRequest request);

    T createBucketService(final BackupCommitLogsOperationRequest request);

    T createBucketService(final RestoreOperationRequest request);

    T createBucketService(final RestoreCommitLogsOperationRequest request);
}
