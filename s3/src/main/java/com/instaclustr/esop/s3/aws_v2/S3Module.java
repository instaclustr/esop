package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.AbstractModule;
import com.instaclustr.esop.SPIModule;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

public class S3Module extends AbstractModule implements SPIModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "s3",
                        S3Restorer.class,
                        S3Backuper.class,
                        S3BucketService.class);
    }

    @Override
    public AbstractModule getGuiceModule() {
        return this;
    }
}
