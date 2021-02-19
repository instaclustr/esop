package com.instaclustr.esop.impl.remove;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class RemoveBackupModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "remove-backup",
                                 RemoveBackupRequest.class,
                                 RemoveBackupOperation.class);
    }
}
