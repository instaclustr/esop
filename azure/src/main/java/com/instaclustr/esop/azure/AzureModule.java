package com.instaclustr.esop.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.SPIModule;
import com.instaclustr.esop.impl.AbstractOperationRequest;

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
    BlobServiceClientFactory provideBlobContainerClientFactory() {
        return new BlobServiceClientFactory();
    }

    @Override
    public AbstractModule getGuiceModule() {
        return this;
    }

    public static class BlobServiceClientFactory {
        public BlobServiceClient build(final AbstractOperationRequest operationRequest) throws AzureModuleException {
            StorageSharedKeyCredential credential = provideSharedKeyCredential();
            return new BlobServiceClientBuilder()
                    .endpoint(provideAzureBlobStorageEndpoint(credential.getAccountName(), operationRequest.insecure))
                    .credential(credential)
                    .buildClient();
        }

        private StorageSharedKeyCredential provideSharedKeyCredential() throws AzureModuleException {
            return resolveCredentialsFromEnvProperties();
        }

        private StorageSharedKeyCredential resolveCredentialsFromEnvProperties() {
            return new StorageSharedKeyCredential(System.getenv("AZURE_STORAGE_ACCOUNT"), System.getenv("AZURE_STORAGE_KEY"));
        }

        private String provideAzureBlobStorageEndpoint(final String accountName, final boolean useHttp) {
            String endpoint = System.getenv("AZURE_STORAGE_ENDPOINT");
            if (endpoint != null && !endpoint.isEmpty()) {
                return endpoint;
            }

            String schema = useHttp ? "http" : "https";
            return String.format("%s://%s.blob.core.windows.net", schema, accountName);
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
