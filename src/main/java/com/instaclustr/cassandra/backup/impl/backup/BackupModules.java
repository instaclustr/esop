package com.instaclustr.cassandra.backup.impl.backup;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class BackupModules {

    public static final class BackupModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "backup",
                                     BackupOperationRequest.class,
                                     BackupOperation.class);
        }
    }

    public static final class CommitlogBackupModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "commitlog-backup",
                                     BackupCommitLogsOperationRequest.class,
                                     BackupCommitLogsOperation.class);
        }
    }
}
