package com.instaclustr.esop.s3.oracle;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.guice.BackupRestoreBindings;
import com.instaclustr.esop.s3.TransferManagerFactory;
import io.kubernetes.client.apis.CoreV1Api;

public class OracleModule extends AbstractModule {

    @Override
    protected void configure() {
        BackupRestoreBindings.installBindings(binder(),
                                              "oracle",
                                              OracleRestorer.class,
                                              OracleBackuper.class,
                                              OracleBucketService.class);
    }

    @Provides
    @Singleton
    public OracleS3TransferManagerFactory provideTransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new OracleS3TransferManagerFactory(coreV1ApiProvider);
    }

    public static final class OracleS3TransferManagerFactory extends TransferManagerFactory {

        public OracleS3TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            super(coreV1ApiProvider, true);
        }
    }
}
