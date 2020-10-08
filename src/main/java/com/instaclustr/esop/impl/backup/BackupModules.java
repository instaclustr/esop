package com.instaclustr.esop.impl.backup;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.util.Types.newParameterizedType;
import static com.instaclustr.operations.OperationBindings.installOperationBindings;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.TypeLiteral;
import com.instaclustr.esop.impl.backup.coordination.DefaultBackupOperationCoordinator;
import com.instaclustr.guice.ServiceBindings;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;

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

    public static final class UploadingModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ListeningExecutorService.class).annotatedWith(UploadingFinisher.class).toInstance(new FixedTasksExecutorSupplier().get(100));

            ServiceBindings.bindService(binder(), UploadTracker.class);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface UploadingFinisher {

    }
}
