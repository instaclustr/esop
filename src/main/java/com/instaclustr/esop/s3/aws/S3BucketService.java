package com.instaclustr.esop.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.BaseS3BucketService;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;

public class S3BucketService extends BaseS3BucketService {

    @AssistedInject
    public S3BucketService(final S3TransferManagerFactory transferManagerFactory,
                           @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3TransferManagerFactory transferManagerFactory,
                           @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3TransferManagerFactory transferManagerFactory,
                           @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3TransferManagerFactory transferManagerFactory,
                           @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}
