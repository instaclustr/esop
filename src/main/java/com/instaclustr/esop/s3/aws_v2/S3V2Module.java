package com.instaclustr.esop.s3.aws_v2;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

public class S3V2Module extends AbstractModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "s3",
                        S3Restorer.class,
                        S3Backuper.class,
                        S3BucketService.class);
    }

    @Provides
    @Singleton
    public S3Clients provideS3Clients() {
        return new S3ClientsFactory().build(new S3ConfigurationResolver());
    }
}
