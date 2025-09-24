package com.instaclustr.esop.guice;

import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;

public interface RestorerFactory<RESTORER extends Restorer> {
    RESTORER createDeletingInteractor(final RemoveBackupRequest removeBackupRequest);
    RESTORER createListingInteractor(final ListOperationRequest listOperationRequest);
    RESTORER createRestorer(final RestoreOperationRequest restoreOperationRequest);
    RESTORER createCommitLogRestorer(final RestoreCommitLogsOperationRequest restoreCommitLogsOperationRequest);
}
