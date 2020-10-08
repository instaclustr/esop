package com.instaclustr.esop.s3.oracle;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.s3.BaseS3BucketService;
import com.instaclustr.esop.s3.oracle.OracleModule.OracleS3TransferManagerFactory;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;

public class OracleBucketService extends BaseS3BucketService {

    @AssistedInject
    public OracleBucketService(final OracleS3TransferManagerFactory transferManagerFactory,
                               @Assisted final BackupOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleBucketService(final OracleS3TransferManagerFactory transferManagerFactory,
                               @Assisted final BackupCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleBucketService(final OracleS3TransferManagerFactory transferManagerFactory,
                               @Assisted final RestoreOperationRequest request) {
        super(transferManagerFactory, request);
    }

    @AssistedInject
    public OracleBucketService(final OracleS3TransferManagerFactory transferManagerFactory,
                               @Assisted final RestoreCommitLogsOperationRequest request) {
        super(transferManagerFactory, request);
    }
}