package com.instaclustr.esop.s3.v1;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.inject.Provider;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration;
import com.instaclustr.kubernetes.KubernetesHelper;
import io.kubernetes.client.apis.CoreV1Api;

import static com.instaclustr.kubernetes.KubernetesHelper.isRunningAsClient;

public class TransferManagerFactory {

    private final Provider<CoreV1Api> coreV1ApiProvider;
    private final boolean enablePathStyleAccess;

    public TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        this(coreV1ApiProvider, false);
    }

    public TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider, final boolean enablePathStyleAccess) {
        this.coreV1ApiProvider = coreV1ApiProvider;
        this.enablePathStyleAccess = enablePathStyleAccess;
    }

    public TransferManager build(final AbstractOperationRequest operationRequest) {
        return build(operationRequest, new S3ConfigurationResolver());
    }

    public TransferManager build(AbstractOperationRequest operationRequest, S3ConfigurationResolver configurationResolver) {
        final AmazonS3 amazonS3 = provideAmazonS3(coreV1ApiProvider, operationRequest, configurationResolver);
        return TransferManagerBuilder.standard().withS3Client(amazonS3).build();
    }

    public boolean isRunningInKubernetes() {
        return KubernetesHelper.isRunningInKubernetes() || isRunningAsClient();
    }

    protected AmazonS3 provideAmazonS3(final Provider<CoreV1Api> coreV1ApiProvider,
                                       final AbstractOperationRequest operationRequest,
                                       S3ConfigurationResolver configurationResolver) {

        final S3Configuration s3Conf = configurationResolver.resolveS3Configuration(coreV1ApiProvider, operationRequest);

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

        if (s3Conf.awsPathStyleAccessEnabled != null) {
            builder.withPathStyleAccessEnabled(s3Conf.awsPathStyleAccessEnabled);
        } else if (enablePathStyleAccess) {
            // for being able to work with Oracle "s3"
            builder.enablePathStyleAccess();
        }

        if (operationRequest.insecure || (operationRequest.proxySettings != null && operationRequest.proxySettings.useProxy)) {
            final ClientConfiguration clientConfiguration = new ClientConfiguration();

            if (operationRequest.insecure) {
                clientConfiguration.withProtocol(Protocol.HTTP);
            }

            if (operationRequest.proxySettings != null && operationRequest.proxySettings.useProxy) {

                if (operationRequest.proxySettings.proxyProtocol != null) {
                    clientConfiguration.setProxyProtocol(operationRequest.proxySettings.proxyProtocol);
                }

                if (operationRequest.proxySettings.proxyHost != null) {
                    clientConfiguration.setProxyHost(operationRequest.proxySettings.proxyHost);
                }

                if (operationRequest.proxySettings.proxyPort != null) {
                    clientConfiguration.setProxyPort(operationRequest.proxySettings.proxyPort);
                }

                if (operationRequest.proxySettings.proxyPassword != null) {
                    clientConfiguration.setProxyPassword(operationRequest.proxySettings.proxyPassword);
                }

                if (operationRequest.proxySettings.proxyUsername != null) {
                    clientConfiguration.setProxyUsername(operationRequest.proxySettings.proxyUsername);
                }
            }

            builder.withClientConfiguration(clientConfiguration);
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
}
