package com.instaclustr.esop.impl.list;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class ListModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "list",
                                 ListOperationRequest.class,
                                 ListOperation.class);
    }
}
