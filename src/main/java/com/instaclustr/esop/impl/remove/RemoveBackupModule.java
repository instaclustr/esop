package com.instaclustr.esop.impl.remove;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class RemoveBackupModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "remove-backup",
                                 RemoveBackupRequest.class,
                                 RemoveBackupOperation.class);
    }
}
