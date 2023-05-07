package com.instaclustr.esop.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.esop.s3.v1.BaseS3Backuper;

public class S3Backuper extends BaseS3Backuper {

    @AssistedInject
    public S3Backuper(final S3TransferManagerFactory transferManagerFactory,
                      @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public S3Backuper(final S3TransferManagerFactory transferManagerFactory,
                      @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}
