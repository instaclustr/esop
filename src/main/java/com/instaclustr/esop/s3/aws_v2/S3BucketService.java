package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.v2.BaseS3BucketService;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;

public class S3BucketService extends BaseS3BucketService {

    @AssistedInject
    public S3BucketService(final S3ClientsFactory s3ClientsFactory,
                           @Assisted final BackupOperationRequest request) {
        super(s3ClientsFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3ClientsFactory s3ClientsFactory,
                           @Assisted final BackupCommitLogsOperationRequest request) {
        super(s3ClientsFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3ClientsFactory s3ClientsFactory,
                           @Assisted final RestoreOperationRequest request) {
        super(s3ClientsFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3ClientsFactory s3ClientsFactory,
                           @Assisted final RestoreCommitLogsOperationRequest request) {
        super(s3ClientsFactory, request);
    }

    @AssistedInject
    public S3BucketService(final S3ClientsFactory s3ClientsFactory,
                           @Assisted final ListOperationRequest request) {
        super(s3ClientsFactory, request);
    }
}