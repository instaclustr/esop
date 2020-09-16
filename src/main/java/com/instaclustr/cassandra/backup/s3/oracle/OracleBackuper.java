package com.instaclustr.cassandra.backup.s3.oracle;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.s3.BaseS3Backuper;
import com.instaclustr.cassandra.backup.s3.oracle.OracleModule.OracleS3TransferManagerFactory;

public class OracleBackuper extends BaseS3Backuper {

    @AssistedInject
    public OracleBackuper(final OracleS3TransferManagerFactory transferManagerFactory,
                          @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleBackuper(final OracleS3TransferManagerFactory transferManagerFactory,
                          @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}