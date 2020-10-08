package com.instaclustr.esop.local;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

import com.google.inject.AbstractModule;

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
