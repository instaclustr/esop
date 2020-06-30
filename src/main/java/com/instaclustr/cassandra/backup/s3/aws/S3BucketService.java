package com.instaclustr.cassandra.backup.s3.aws;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3BucketService;
import com.instaclustr.cassandra.backup.s3.aws.S3Module.S3TransferManagerFactory;

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
}
