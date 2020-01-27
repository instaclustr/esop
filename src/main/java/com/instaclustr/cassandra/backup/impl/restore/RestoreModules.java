package com.instaclustr.cassandra.backup.impl.restore;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class RestoreModules {

    public static final class RestoreModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "restore",
                                     RestoreOperationRequest.class,
                                     RestoreOperation.class);
        }
    }

    public static final class RestoreCommitlogModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "commitlog-restore",
                                     RestoreCommitLogsOperationRequest.class,
                                     RestoreCommitLogsOperation.class);
        }
    }
}
