package com.instaclustr.cassandra.backup.gcp;

import static com.instaclustr.cassandra.backup.guice.BackupRestoreBindings.installBindings;
import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.cassandra.backup.impl.AbstractOperationRequest;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.kubernetes.SecretReader;
import io.kubernetes.client.apis.CoreV1Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCPModule extends AbstractModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "gcp",
                        GCPRestorer.class,
                        GCPBackuper.class,
                        GCPBucketService.class);
    }

    @Provides
    @Singleton
    GoogleStorageFactory provideGoogleStorageFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new GoogleStorageFactory(coreV1ApiProvider);
    }

    public static class GoogleStorageFactory {

        private static final Logger logger = LoggerFactory.getLogger(GoogleStorageFactory.class);

        private final Provider<CoreV1Api> coreV1ApiProvider;

        public GoogleStorageFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            this.coreV1ApiProvider = coreV1ApiProvider;
        }

        public Storage build(final AbstractOperationRequest operationRequest) {

            if (KubernetesHelper.isRunningInKubernetes() || KubernetesHelper.isRunningAsClient()) {
                final GoogleCredentials credentials = resolveGoogleCredentials(operationRequest);
                return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            } else {
                String googleAppCredentialsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

                if (googleAppCredentialsPath == null) {
                    logger.warn("You are not running in Kubernetes and GOOGLE_APPLICATION_CREDENTIALS environment property was not set, "
                                    + "going to try system property google.application.credentials");

                    googleAppCredentialsPath = System.getProperty("google.application.credentials");

                    if (googleAppCredentialsPath == null) {
                        throw new GCPModuleException("You are not running in Kubernetes and google.application.credentials system property was not set.");
                    }
                }

                if (!Files.exists(Paths.get(googleAppCredentialsPath))) {
                    throw new GCPModuleException(format("You are not running in Kubernetes and file %s does not exist!", googleAppCredentialsPath));
                }
                return StorageOptions.getDefaultInstance().getService();
            }
        }

        private GoogleCredentials resolveGoogleCredentials(final AbstractOperationRequest operationRequest) {
            final String dataKey = "gcp";

            final String namespace = operationRequest.resolveKubernetesNamespace();
            final String secretName = operationRequest.resolveSecretName();

            try {
                Optional<byte[]> gcpCredentials = new SecretReader(coreV1ApiProvider).read(namespace,
                                                                                           secretName,
                                                                                           dataKey);

                if (!gcpCredentials.isPresent()) {
                    throw new GCPModuleException(format("GCP credentials from Kubernetes namespace %s from secret %s under key %s were not set.",
                                                        namespace,
                                                        secretName,
                                                        dataKey));
                }

                return GoogleCredentials.fromStream(new ByteArrayInputStream(gcpCredentials.get()))
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
            } catch (final Exception ex) {
                throw new GCPModuleException(format("Unable to resolve data for key %s on secret %s", dataKey, secretName), ex);
            }
        }
    }

    public static final class GCPModuleException extends RuntimeException {

        public GCPModuleException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public GCPModuleException(final String message) {
            super(message);
        }
    }
}
