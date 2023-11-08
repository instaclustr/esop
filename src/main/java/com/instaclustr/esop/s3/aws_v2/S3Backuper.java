package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v2.BaseS3Backuper;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;

public class S3Backuper extends BaseS3Backuper
{
    @AssistedInject
    public S3Backuper(@Assisted final BackupOperationRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver(request)), request);
    }

    @AssistedInject
    public S3Backuper(@Assisted final BackupCommitLogsOperationRequest request) {
        super(new S3ClientsFactory().build(new S3ConfigurationResolver(request)), request);
    }
}
