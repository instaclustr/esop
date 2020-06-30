package com.instaclustr.cassandra.backup.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Restorer;
import com.instaclustr.cassandra.backup.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class S3Restorer extends BaseS3Restorer {

    @AssistedInject
    public S3Restorer(final S3TransferManagerFactory transferManagerFactory,
                      final ExecutorServiceSupplier executorServiceSupplier,
                      @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, executorServiceSupplier, request);
    }

    @AssistedInject
    public S3Restorer(final S3TransferManagerFactory transferManagerFactory,
                      final ExecutorServiceSupplier executorServiceSupplier,
                      @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, executorServiceSupplier, request);
    }
}
