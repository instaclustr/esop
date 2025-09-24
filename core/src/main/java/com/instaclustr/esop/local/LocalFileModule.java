package com.instaclustr.esop.local;

import com.google.inject.AbstractModule;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

public class LocalFileModule extends AbstractModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "file",
                        LocalFileRestorer.class,
                        LocalFileBackuper.class,
                        LocalBucketService.class);
    }
}
