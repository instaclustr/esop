package com.instaclustr.cassandra.backup.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Backuper;
import com.instaclustr.cassandra.backup.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class S3Backuper extends BaseS3Backuper {

    @AssistedInject
    public S3Backuper(final S3TransferManagerFactory transferManagerFactory,
                      final ExecutorServiceSupplier executorSupplier,
                      @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, executorSupplier, request);
    }

    @AssistedInject
    public S3Backuper(final S3TransferManagerFactory transferManagerFactory,
                      final ExecutorServiceSupplier executorSupplier,
                      @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, executorSupplier, request);
    }
}
