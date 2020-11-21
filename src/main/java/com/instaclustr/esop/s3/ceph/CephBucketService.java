package com.instaclustr.esop.s3.ceph;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.BaseS3BucketService;
import com.instaclustr.esop.s3.ceph.CephModule.CephS3TransferManagerFactory;

public class CephBucketService extends BaseS3BucketService {

    @AssistedInject
    public CephBucketService(final CephS3TransferManagerFactory transferManagerFactory,
                             @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephBucketService(final CephS3TransferManagerFactory transferManagerFactory,
                             @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephBucketService(final CephS3TransferManagerFactory transferManagerFactory,
                             @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephBucketService(final CephS3TransferManagerFactory transferManagerFactory,
                             @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}
