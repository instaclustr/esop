package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.v2.BaseS3BucketService;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

public class S3BucketService extends BaseS3BucketService {

    public S3BucketService(S3Clients s3Clients) {
        super(s3Clients);
    }

    @AssistedInject
    public S3BucketService(final S3Clients s3Clients,
                           @Assisted final BackupOperationRequest request) {
        super(s3Clients);
    }

    @AssistedInject
    public S3BucketService(final S3Clients s3Clients,
                           @Assisted final BackupCommitLogsOperationRequest request) {
        super(s3Clients);
    }

    @AssistedInject
    public S3BucketService(final S3Clients s3Clients,
                           @Assisted final RestoreOperationRequest request) {
        super(s3Clients);
    }

    @AssistedInject
    public S3BucketService(final S3Clients s3Clients,
                           @Assisted final RestoreCommitLogsOperationRequest request) {
        super(s3Clients);
    }

    @AssistedInject
    public S3BucketService(final S3Clients s3Clients,
                           @Assisted final ListOperationRequest request) {
        super(s3Clients);
    }
}
