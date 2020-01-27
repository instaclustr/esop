package com.instaclustr.cassandra.backup.aws;

import static com.instaclustr.cassandra.backup.guice.BackupRestoreBindings.installBindings;
import static com.instaclustr.kubernetes.KubernetesHelper.isRunningAsClient;
import static java.lang.String.format;

import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.instaclustr.cassandra.backup.impl.KubernetesAwareRequest;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.kubernetes.KubernetesSecretsReader;
import com.instaclustr.kubernetes.SecretReader;
import io.kubernetes.client.apis.CoreV1Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Module extends AbstractModule {

    @Override
    protected void configure() {
        installBindings(binder(),
                        "s3",
                        S3Restorer.class,
                        S3Backuper.class);
    }

    @Provides
    TransferManagerFactory provideTransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new TransferManagerFactory(coreV1ApiProvider);
    }

    public static class TransferManagerFactory {

        private static final Logger logger = LoggerFactory.getLogger(TransferManagerFactory.class);

        private final Provider<CoreV1Api> coreV1ApiProvider;

        public TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            this.coreV1ApiProvider = coreV1ApiProvider;
        }

        public TransferManager build(final KubernetesAwareRequest operationRequest) {
            final AmazonS3 amazonS3 = provideAmazonS3(coreV1ApiProvider, operationRequest);
            return TransferManagerBuilder.standard().withS3Client(amazonS3).build();
        }

        public boolean isRunningInKubernetes() {
            return KubernetesHelper.isRunningInKubernetes() || isRunningAsClient();
        }

        private AmazonS3 provideAmazonS3(final Provider<CoreV1Api> coreV1ApiProvider, final KubernetesAwareRequest operationRequest) {

            final S3Configuration s3Conf = resolveS3Configuration(coreV1ApiProvider, operationRequest);

            final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

            if (s3Conf.awsEndpoint != null) {
                // AWS_REGION must be set if AWS_ENDPOINT is set
                if (s3Conf.awsRegion == null) {
                    throw new IllegalArgumentException("AWS_REGION must be set if AWS_ENDPOINT is set.");
                }

                builder.withEndpointConfiguration(new EndpointConfiguration(s3Conf.awsEndpoint, s3Conf.awsRegion.toLowerCase()));
            } else if (s3Conf.awsRegion != null) {
                builder.withRegion(Regions.fromName(s3Conf.awsRegion.toLowerCase()));
            }

            // if we are not running against Kubernetes, credentials should be fetched from ~/.aws/...
            if (isRunningInKubernetes()) {
                // it is possible that we have not set any secrets for s3 so the last
                // resort is to fallback to AWS instance credentials.
                if (s3Conf.awsAccessKeyId != null && s3Conf.awsSecretKey != null) {
                    builder.setCredentials(new AWSCredentialsProvider() {
                        @Override
                        public AWSCredentials getCredentials() {
                            return new AWSCredentials() {
                                @Override
                                public String getAWSAccessKeyId() {
                                    return s3Conf.awsAccessKeyId;
                                }

                                @Override
                                public String getAWSSecretKey() {
                                    return s3Conf.awsSecretKey;
                                }
                            };
                        }

                        @Override
                        public void refresh() {
                        }
                    });
                }
            }

            return builder.build();
        }

        private S3Configuration resolveS3Configuration(final Provider<CoreV1Api> coreV1ApiProvider, final KubernetesAwareRequest operationRequest) {
            if (isRunningInKubernetes()) {
                try {
                    return resolveS3ConfigurationFromK8S(coreV1ApiProvider, operationRequest);
                } catch (final S3ModuleException ex) {
                    logger.warn(String.format("Unable to resolve credentials for S3 from Kubernetes secret %s. The last chance "
                                                  + "for this container to authenticate is to use AWS instance credentials."), ex);
                    return new S3Configuration();
                }
            } else {
                return resolveS3ConfigurationFromEnvProperties();
            }
        }

        private S3Configuration resolveS3ConfigurationFromK8S(final Provider<CoreV1Api> coreV1ApiProvider, final KubernetesAwareRequest operationRequest) {

            try {

                final String namespace = resolveKubernetesKeyspace(operationRequest);
                final SecretReader secretReader = new SecretReader(coreV1ApiProvider);

                return secretReader.readIntoObject(namespace,
                                                   operationRequest.getSecretName(),
                                                   secret -> {
                                                       final Map<String, byte[]> data = secret.getData();

                                                       final S3Configuration s3Configuration = new S3Configuration();

                                                       final byte[] awsendpoint = data.get("awsendpoint");
                                                       final byte[] awsregion = data.get("awsregion");
                                                       final byte[] awssecretaccesskey = data.get("awssecretaccesskey");
                                                       final byte[] awsaccesskeyid = data.get("awsaccesskeyid");

                                                       if (awsendpoint != null) {
                                                           s3Configuration.awsEndpoint = new String(awsendpoint);
                                                       }

                                                       if (awsregion != null) {
                                                           s3Configuration.awsRegion = new String(awsregion);
                                                       }

                                                       if (awsaccesskeyid != null) {
                                                           s3Configuration.awsAccessKeyId = new String(awsaccesskeyid);
                                                       } else {
                                                           throw new S3ModuleException(format("Secret %s does not contain any entry with key 'awsaccesskeyid'.",
                                                                                              secret.getMetadata().getName()));
                                                       }

                                                       if (awssecretaccesskey != null) {
                                                           s3Configuration.awsSecretKey = new String(awssecretaccesskey);
                                                       } else {
                                                           throw new S3ModuleException(format("Secret %s does not contain any entry with key 'awssecretaccesskey'.",
                                                                                              secret.getMetadata().getName()));
                                                       }

                                                       return s3Configuration;
                                                   });
            } catch (final Exception ex) {
                throw new S3ModuleException("Unable to resolve S3Configuration for backup / restores from Kubernetes. ", ex);
            }
        }

        private S3Configuration resolveS3ConfigurationFromEnvProperties() {
            final S3Configuration s3Configuration = new S3Configuration();

            s3Configuration.awsRegion = System.getenv("AWS_REGION");
            s3Configuration.awsEndpoint = System.getenv("AWS_ENDPOINT");

            // accesskeyid and awssecretkey will be taken from normal configuration mechanism in ~/.aws/ ...
            // s3Configuration.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
            // s3Configuration.awsSecretKey = System.getenv("AWS_SECRET_KEY");

            return s3Configuration;
        }

        private String resolveKubernetesKeyspace(final KubernetesAwareRequest operationRequest) {
            if (operationRequest.getNamespace() != null) {
                return operationRequest.getNamespace();
            } else {
                return KubernetesSecretsReader.readNamespace();
            }
        }

        private static final class S3Configuration {

            public String awsRegion;
            public String awsEndpoint;
            public String awsAccessKeyId;
            public String awsSecretKey;
        }
    }

    public static final class S3ModuleException extends RuntimeException {

        public S3ModuleException(String message, Throwable cause) {
            super(message, cause);
        }

        public S3ModuleException(String message) {
            super(message);
        }
    }
}
