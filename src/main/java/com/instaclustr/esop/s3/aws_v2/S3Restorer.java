package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v2.BaseS3Restorer;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;

public class S3Restorer extends BaseS3Restorer {

    @AssistedInject
    public S3Restorer(@Assisted final RestoreOperationRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver(request)), request);
    }

    @AssistedInject
    public S3Restorer(@Assisted final RestoreCommitLogsOperationRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver(request)), request);
    }

    @AssistedInject
    public S3Restorer(@Assisted final ListOperationRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver()), request);
    }

    @AssistedInject
    public S3Restorer(@Assisted final RemoveBackupRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver()), request);
    }
}
