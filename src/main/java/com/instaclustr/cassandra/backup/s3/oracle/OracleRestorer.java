package com.instaclustr.cassandra.backup.s3.oracle;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Restorer;
import com.instaclustr.cassandra.backup.s3.oracle.OracleModule.OracleS3TransferManagerFactory;

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
}