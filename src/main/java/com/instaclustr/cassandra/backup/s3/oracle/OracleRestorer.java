package com.instaclustr.cassandra.backup.s3.oracle;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Restorer;
import com.instaclustr.cassandra.backup.s3.oracle.OracleModule.OracleS3TransferManagerFactory;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

public class OracleRestorer extends BaseS3Restorer {

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          final ExecutorServiceSupplier executorServiceSupplier,
                          @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, executorServiceSupplier, request);
    }

    @AssistedInject
    public OracleRestorer(final OracleS3TransferManagerFactory transferManagerFactory,
                          final ExecutorServiceSupplier executorServiceSupplier,
                          @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, executorServiceSupplier, request);
    }
}