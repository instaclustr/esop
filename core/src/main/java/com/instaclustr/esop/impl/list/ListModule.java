package com.instaclustr.esop.impl.list;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class ListModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "list",
                                 ListOperationRequest.class,
                                 ListOperation.class);
    }
}
