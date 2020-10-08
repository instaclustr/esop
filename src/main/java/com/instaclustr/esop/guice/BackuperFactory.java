package com.instaclustr.esop.guice;

import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;

public interface BackuperFactory<BACKUPER extends Backuper> {
    BACKUPER createBackuper(final BackupOperationRequest backupOperationRequest);
    BACKUPER createCommitLogBackuper(final BackupCommitLogsOperationRequest backupCommitLogsOperationRequest);
}
