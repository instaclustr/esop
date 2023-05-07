package com.instaclustr.esop.s3.minio;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.minio.MinioModule.MinioS3TransferManagerFactory;
import com.instaclustr.esop.s3.v1.BaseS3Backuper;

public class MinioBackuper extends BaseS3Backuper {

    @AssistedInject
    public MinioBackuper(final MinioS3TransferManagerFactory transferManagerFactory,
                         @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public MinioBackuper(final MinioS3TransferManagerFactory transferManagerFactory,
                         @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}