package com.instaclustr.esop.s3.oracle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.oracle.OracleModule.OracleS3TransferManagerFactory;
import com.instaclustr.esop.s3.v1.BaseS3Restorer;

public class OracleRestorer extends BaseS3Restorer {

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          final ObjectMapper objectMapper,
                          @Assisted final ListOperationRequest request) {
        super(transferManagerFactory, objectMapper, request);
    }

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          final ObjectMapper objectMapper,
                          @Assisted final RemoveBackupRequest request) {
        super(transferManagerFactory, objectMapper, request);
    }
}