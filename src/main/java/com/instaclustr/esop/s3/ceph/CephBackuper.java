package com.instaclustr.esop.s3.ceph;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.BaseS3Backuper;
import com.instaclustr.esop.s3.ceph.CephModule.CephS3TransferManagerFactory;

public class CephBackuper extends BaseS3Backuper {

    @AssistedInject
    public CephBackuper(final CephS3TransferManagerFactory transferManagerFactory,
                        @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephBackuper(final CephS3TransferManagerFactory transferManagerFactory,
                        @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}
