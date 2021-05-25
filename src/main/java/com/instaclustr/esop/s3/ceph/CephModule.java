package com.instaclustr.esop.s3.ceph;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.guice.BackupRestoreBindings;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.s3.TransferManagerFactory;
import io.kubernetes.client.apis.CoreV1Api;

public class CephModule extends AbstractModule {

    @Override
    protected void configure() {
        BackupRestoreBindings.installBindings(binder(),
                                              "ceph",
                                              CephRestorer.class,
                                              CephBackuper.class,
                                              CephBucketService.class);
    }

    @Provides
    @Singleton
    public CephS3TransferManagerFactory provideTransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
        return new CephS3TransferManagerFactory(coreV1ApiProvider);
    }

    public static final class CephS3TransferManagerFactory extends TransferManagerFactory {

        private static final String DEFAULT_AWS_REGION = "us-east-1";

        public CephS3TransferManagerFactory(final Provider<CoreV1Api> coreV1ApiProvider) {
            super(coreV1ApiProvider, false);
        }

        @Override
        protected AmazonS3 provideAmazonS3(final Provider<CoreV1Api> coreV1ApiProvider, final AbstractOperationRequest operationRequest) {

            final S3Configuration s3Conf = resolveS3Configuration(coreV1ApiProvider, operationRequest);

            final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

            if (s3Conf.awsEndpoint != null) {
                builder.withEndpointConfiguration(new EndpointConfiguration(s3Conf.awsEndpoint, DEFAULT_AWS_REGION));
            } else {
                throw new IllegalStateException("You have to specify endpoint for Ceph module, either via "
                        + "AWS_ENDPOINT environment variable or via awsendpoint K8S property in secret");
            }

            if(s3Conf.awsPathStyleAccessEnabled != null) {
                builder.withPathStyleAccessEnabled(s3Conf.awsPathStyleAccessEnabled);
            }

            AWSCredentials credentials = null;

            if (s3Conf.awsSecretKey != null && s3Conf.awsAccessKeyId != null) {
                credentials = new BasicAWSCredentials(s3Conf.awsAccessKeyId, s3Conf.awsSecretKey);
            }

            ClientConfiguration clientConfig = new ClientConfiguration();

            if (operationRequest.insecure) {
                clientConfig.withProtocol(Protocol.HTTP);
            }

            if (credentials != null) {
                builder.withCredentials(new AWSCredentialsProvider() {
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
            } else {
                builder.withClientConfiguration(clientConfig);
            }

            return builder.build();
        }
    }
}
