package com.instaclustr.cassandra.backup.impl._import;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class ImportModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "import",
                                 ImportOperationRequest.class,
                                 ImportOperation.class);
    }
}
