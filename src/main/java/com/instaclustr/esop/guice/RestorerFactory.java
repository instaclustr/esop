package com.instaclustr.esop.guice;

import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;

public interface RestorerFactory<RESTORER extends Restorer> {
    RESTORER createRestorer(final RestoreOperationRequest restoreOperationRequest);
    RESTORER createCommitLogRestorer(final RestoreCommitLogsOperationRequest restoreCommitLogsOperationRequest);
}
