package com.instaclustr.esop.impl.restore;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import com.google.common.util.concurrent.ListeningExecutorService;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.instaclustr.esop.impl.restore.coordination.DefaultRestoreOperationCoordinator;
import com.instaclustr.esop.impl.restore.strategy.HardlinkingRestorationStrategy;
import com.instaclustr.esop.impl.restore.strategy.ImportingRestorationStrategy;
import com.instaclustr.esop.impl.restore.strategy.InPlaceRestorationStrategy;
import com.instaclustr.guice.ServiceBindings;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.util.Types.newParameterizedType;
import static com.instaclustr.operations.OperationBindings.installOperationBindings;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class RestoreModules {

    public static final class RestoreModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "restore",
                                     RestoreOperationRequest.class,
                                     RestoreOperation.class);

            @SuppressWarnings("unchecked") final TypeLiteral<OperationCoordinator<RestoreOperationRequest>> operationCoordinator =
                (TypeLiteral<OperationCoordinator<RestoreOperationRequest>>) TypeLiteral.get(newParameterizedType(OperationCoordinator.class, RestoreOperationRequest.class));

            newOptionalBinder(binder(), operationCoordinator).setDefault().to(DefaultRestoreOperationCoordinator.class);
        }
    }

    public static final class RestorationStrategyModule extends AbstractModule {

        @Override
        protected void configure() {
            final Multibinder<RestorationStrategy> restorationStrategyMultibinder = Multibinder.newSetBinder(binder(), RestorationStrategy.class);

            restorationStrategyMultibinder.addBinding().to(InPlaceRestorationStrategy.class);
            restorationStrategyMultibinder.addBinding().to(HardlinkingRestorationStrategy.class);
            restorationStrategyMultibinder.addBinding().to(ImportingRestorationStrategy.class);
        }

        @Provides
        public RestorationStrategyResolver getRestorationStrategyProvider(Set<RestorationStrategy> restorationStrategySet,
                                                                          Provider<CassandraJMXService> cassandraJMXServiceProvider) {
            return new RestorationStrategyResolverImpl(restorationStrategySet, cassandraJMXServiceProvider);
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

    public static final class DownloadingModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ListeningExecutorService.class).annotatedWith(Downloading.class).toInstance(new FixedTasksExecutorSupplier().get(100));
            bind(ListeningExecutorService.class).annotatedWith(DownloadingFinisher.class).toInstance(new FixedTasksExecutorSupplier().get(100));

            ServiceBindings.bindService(binder(), DownloadTracker.class);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface Downloading {

    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface DownloadingFinisher {

    }
}
