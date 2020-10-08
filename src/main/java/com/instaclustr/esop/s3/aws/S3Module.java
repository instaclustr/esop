package com.instaclustr.esop.s3.aws;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.s3.TransferManagerFactory;
import io.kubernetes.client.apis.CoreV1Api;

public class S3Module extends AbstractModule {

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
    public S3TransferManagerFactory provideTransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new S3TransferManagerFactory(coreV1ApiProvider);
    }

    public static final class S3TransferManagerFactory extends TransferManagerFactory {

        public S3TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            super(coreV1ApiProvider);
        }
    }
}
