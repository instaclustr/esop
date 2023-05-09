package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.v2.BaseS3Restorer;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

public class S3Restorer extends BaseS3Restorer {

    @AssistedInject
    public S3Restorer(final S3Clients s3Clients,
                      @Assisted final RestoreOperationRequest request) {
        super(s3Clients, request);
    }

    @AssistedInject
    public S3Restorer(final S3Clients s3Clients,
                      @Assisted final RestoreCommitLogsOperationRequest request) {
        super(s3Clients, request);
    }

    @AssistedInject
    public S3Restorer(final S3Clients s3Clients,
                      @Assisted final ListOperationRequest request) {
        super(s3Clients, request);
    }

    @AssistedInject
    public S3Restorer(final S3Clients s3Clients,
                      @Assisted final RemoveBackupRequest request) {
        super(s3Clients, request);
    }
}
