package com.instaclustr.esop.azure;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.SPIModule;
import com.instaclustr.esop.impl.AbstractOperationRequest;

import java.time.Duration;
import java.util.Optional;

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
        public static final String BOTH_CREDENTIALS_SET_ERROR_MESSAGE = "Both AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY are set. Please set only one method of authentication.";
        public static final String NO_AZURE_CREDENTIALS_ERROR_MESSAGE = "Azure credentials are not set. Please set either AZURE_STORAGE_CONNECTION_STRING or both AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY environment variables.";

        public BlobServiceClient build(final AbstractOperationRequest operationRequest) throws AzureModuleException {
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

            Optional<String> connectionString = resolveConnectionStringFromEnv();
            Optional<StorageSharedKeyCredential> sharedKeyCredentials = resolveStorageSharedKeyCredentialsFromEnv();

            if (connectionString.isPresent() && sharedKeyCredentials.isPresent()) {
                throw new AzureModuleException(BOTH_CREDENTIALS_SET_ERROR_MESSAGE);
            }

            if (!connectionString.isPresent() && !sharedKeyCredentials.isPresent()) {
                throw new AzureModuleException(NO_AZURE_CREDENTIALS_ERROR_MESSAGE);
            }

            if (connectionString.isPresent()) {
                builder.connectionString(connectionString.get());
            }

            builder.httpClient(HttpClient.createDefault())
                    .retryOptions(new RequestRetryOptions(
                            RetryPolicyType.EXPONENTIAL,
                            5,                    // max retries
                            Duration.ofSeconds(120), // timeout per request
                            null, null, null
                    ));

            sharedKeyCredentials.ifPresent(storageSharedKeyCredential -> builder
                    .credential(storageSharedKeyCredential)
                    .endpoint(provideAzureBlobStorageEndpoint(storageSharedKeyCredential.getAccountName(), operationRequest.insecure)));

            return builder.buildClient();
        }

        /**
         * Resolve connection string from environment variable AZURE_STORAGE_CONNECTION_STRING.
         * @return Optional containing the connection string if set and not empty, otherwise an empty Optional.
         */
        private Optional<String> resolveConnectionStringFromEnv() {
            String connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
            if (connectionString != null && !connectionString.isEmpty()) {
                return Optional.of(connectionString);
            }
            return Optional.empty();
        }

        /**
         * Resolve shared key credentials from environment variables AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY.
         * @return Optional containing StorageSharedKeyCredential if both variables are set and not empty, otherwise an empty Optional.
         */
        private Optional<StorageSharedKeyCredential> resolveStorageSharedKeyCredentialsFromEnv() {
            String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
            String accountKey = System.getenv("AZURE_STORAGE_KEY");
            if (accountName != null && !accountName.isEmpty() && accountKey != null && !accountKey.isEmpty()) {
                return Optional.of(new StorageSharedKeyCredential(accountName, accountKey));
            }
            return Optional.empty();
        }

        private String provideAzureBlobStorageEndpoint(final String accountName, final boolean useHttp) {
            return String.format("%s://%s.blob.core.windows.net", useHttp ? "http" : "https", accountName);
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
