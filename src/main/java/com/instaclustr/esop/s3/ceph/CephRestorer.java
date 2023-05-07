package com.instaclustr.esop.s3.ceph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.ceph.CephModule.CephS3TransferManagerFactory;
import com.instaclustr.esop.s3.v1.BaseS3Restorer;

public class CephRestorer extends BaseS3Restorer {

    @AssistedInject
    public CephRestorer(final CephS3TransferManagerFactory transferManagerFactory,
                        @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephRestorer(final CephS3TransferManagerFactory transferManagerFactory,
                        @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public CephRestorer(final CephS3TransferManagerFactory transferManagerFactory,
                        final ObjectMapper objectMapper,
                        @Assisted final ListOperationRequest request) {
        super(transferManagerFactory, objectMapper, request);
    }

    @AssistedInject
    public CephRestorer(final CephS3TransferManagerFactory transferManagerFactory,
                        final ObjectMapper objectMapper,
                        @Assisted final RemoveBackupRequest request) {
        super(transferManagerFactory, objectMapper, request);
    }
}
