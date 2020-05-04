package com.instaclustr.cassandra.backup.impl.backup.coordination;

import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.ResultGatherer;

public class BackupOperationCoordinator extends OperationCoordinator<BackupOperationRequest> {

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) throws OperationCoordinatorException {
        return null;
    }
}
