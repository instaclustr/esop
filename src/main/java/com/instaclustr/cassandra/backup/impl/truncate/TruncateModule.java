package com.instaclustr.cassandra.backup.impl.truncate;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class TruncateModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "truncate",
                                 TruncateOperationRequest.class,
                                 TruncateOperation.class);
    }
}
