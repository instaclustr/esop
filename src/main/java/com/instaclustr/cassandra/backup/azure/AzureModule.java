package com.instaclustr.cassandra.backup.azure;

import static com.instaclustr.cassandra.backup.guice.BackupRestoreBindings.installBindings;
import static com.instaclustr.kubernetes.KubernetesHelper.isRunningAsClient;
import static java.lang.String.format;

import java.net.URISyntaxException;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.cassandra.backup.impl.KubernetesAwareRequest;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.kubernetes.KubernetesSecretsReader;
import com.instaclustr.kubernetes.SecretReader;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import io.kubernetes.client.apis.CoreV1Api;

public class AzureModule extends AbstractModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "azure",
                        AzureRestorer.class,
                        AzureBackuper.class,
                        AzureBucketService.class);
    }

    @Provides
    @Singleton
    CloudStorageAccountFactory provideCloudStorageAccountFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new CloudStorageAccountFactory(coreV1ApiProvider);
    }

    public static class CloudStorageAccountFactory {

        private final Provider<CoreV1Api> coreV1ApiProvider;

        public CloudStorageAccountFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            this.coreV1ApiProvider = coreV1ApiProvider;
        }

        public CloudStorageAccount build(final KubernetesAwareRequest operationRequest) throws AzureModuleException, URISyntaxException {
            return new CloudStorageAccount(provideStorageCredentialsAccountAndKey(coreV1ApiProvider, operationRequest), true);
        }

        public boolean isRunningInKubernetes() {
            return KubernetesHelper.isRunningInKubernetes() || isRunningAsClient();
        }

        private StorageCredentialsAccountAndKey provideStorageCredentialsAccountAndKey(final Provider<CoreV1Api> coreV1ApiProvider,
                                                                                       final KubernetesAwareRequest operationrequest) throws AzureModuleException {
            if (isRunningInKubernetes()) {
                return resolveCredentialsFromK8S(coreV1ApiProvider, operationrequest);
            } else {
                return resolveCredentialsFromEnvProperties();
            }
        }

        private StorageCredentialsAccountAndKey resolveCredentialsFromEnvProperties() {
            return new StorageCredentialsAccountAndKey(System.getenv("AZURE_STORAGE_ACCOUNT"), System.getenv("AZURE_STORAGE_KEY"));
        }

        private StorageCredentialsAccountAndKey resolveCredentialsFromK8S(final Provider<CoreV1Api> coreV1ApiProvider,
                                                                          final KubernetesAwareRequest operationrequest) {
            try {
                final String namespace = resolveKubernetesKeyspace(operationrequest);
                final SecretReader secretReader = new SecretReader(coreV1ApiProvider);

                return secretReader.readIntoObject(namespace,
                                                   operationrequest.getSecretName(),
                                                   secret -> {
                                                       final Map<String, byte[]> data = secret.getData();

                                                       final byte[] azureStorageAccount = data.get("azurestorageaccount");
                                                       final byte[] azureStorageKey = data.get("azurestoragekey");

                                                       if (azureStorageAccount == null) {
                                                           throw new AzureModuleException(format("Secret %s does not contain any entry with key 'azurestorageaccount'",
                                                                                                 secret.getMetadata().getName()));
                                                       }

                                                       if (azureStorageKey == null) {
                                                           throw new AzureModuleException(format("Secret %s does not contain any entry with key 'azurestoragekey'",
                                                                                                 secret.getMetadata().getName()));
                                                       }

                                                       return new StorageCredentialsAccountAndKey(
                                                           new String(azureStorageAccount),
                                                           new String(azureStorageKey)
                                                       );
                                                   });
            } catch (final Exception ex) {
                throw new AzureModuleException("Unable to resolve Azure credentials for backup / restores from Kubernetes ", ex);
            }
        }

        private String resolveKubernetesKeyspace(final KubernetesAwareRequest operationRequest) {
            if (operationRequest.getNamespace() != null) {
                return operationRequest.getNamespace();
            } else {
                return KubernetesSecretsReader.readNamespace();
            }
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
