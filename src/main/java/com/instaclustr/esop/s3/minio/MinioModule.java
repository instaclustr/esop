package com.instaclustr.esop.s3.minio;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.guice.BackupRestoreBindings;
import com.instaclustr.esop.s3.v1.TransferManagerFactory;
import io.kubernetes.client.apis.CoreV1Api;

public class MinioModule extends AbstractModule {

    @Override
    protected void configure() {
        BackupRestoreBindings.installBindings(binder(),
                                              "minio",
                                              MinioRestorer.class,
                                              MinioBackuper.class,
                                              MinioBucketService.class);
    }

    @Provides
    @Singleton
    public MinioS3TransferManagerFactory provideTransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new MinioS3TransferManagerFactory(coreV1ApiProvider);
    }

    public static final class MinioS3TransferManagerFactory extends TransferManagerFactory {

        public MinioS3TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            super(coreV1ApiProvider, true);
        }
    }
}
