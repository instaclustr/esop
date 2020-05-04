package com.instaclustr.cassandra.backup.impl.backup;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.util.Types.newParameterizedType;
import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.instaclustr.cassandra.backup.impl.backup.coordination.DefaultBackupOperationCoordinator;
import com.instaclustr.operations.OperationCoordinator;

public class BackupModules {

    public static final class BackupModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "backup",
                                     BackupOperationRequest.class,
                                     BackupOperation.class);

            @SuppressWarnings("unchecked") final TypeLiteral<OperationCoordinator<BackupOperationRequest>> operationCoordinator =
                (TypeLiteral<OperationCoordinator<BackupOperationRequest>>) TypeLiteral.get(newParameterizedType(OperationCoordinator.class, BackupOperationRequest.class));

            newOptionalBinder(binder(), operationCoordinator).setDefault().to(DefaultBackupOperationCoordinator.class);
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
