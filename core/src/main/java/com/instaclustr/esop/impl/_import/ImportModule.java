package com.instaclustr.esop.impl._import;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class ImportModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "import",
                                 ImportOperationRequest.class,
                                 ImportOperation.class);
    }
}
