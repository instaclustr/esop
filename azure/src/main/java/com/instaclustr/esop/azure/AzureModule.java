package com.instaclustr.esop.azure;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.SPIModule;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;

import java.net.URISyntaxException;

import static com.instaclustr.esop.guice.BackupRestoreBindings.installBindings;

public class AzureModule extends AbstractModule implements SPIModule
{
    @Override
    protected void configure()
    {
        installBindings(binder(),
                        "azure",
                        AzureRestorer.class,
                        AzureBackuper.class,
                        AzureBucketService.class);
    }

    @Provides
    @Singleton
    CloudStorageAccountFactory provideCloudStorageAccountFactory() {
        return new CloudStorageAccountFactory();
    }

    @Override
    public AbstractModule getGuiceModule() {
        return this;
    }

    public static class CloudStorageAccountFactory {
        public CloudStorageAccount build(final AbstractOperationRequest operationRequest) throws AzureModuleException, URISyntaxException {
            return new CloudStorageAccount(provideStorageCredentialsAccountAndKey(), !operationRequest.insecure);
        }

        private StorageCredentialsAccountAndKey provideStorageCredentialsAccountAndKey() throws AzureModuleException {
            return resolveCredentialsFromEnvProperties();
        }

        private StorageCredentialsAccountAndKey resolveCredentialsFromEnvProperties() {
            return new StorageCredentialsAccountAndKey(System.getenv("AZURE_STORAGE_ACCOUNT"), System.getenv("AZURE_STORAGE_KEY"));
        }
    }

    public static final class AzureModuleException extends RuntimeException {

        public AzureModuleException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public AzureModuleException(final String message) {
            super(message);
        }
    }
}
