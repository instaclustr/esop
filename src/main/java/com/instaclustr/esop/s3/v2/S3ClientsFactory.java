package com.instaclustr.esop.s3.v2;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Provider;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import org.apache.http.client.utils.URIBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.encryption.s3.S3EncryptionClient;

public class S3ClientsFactory {

    private final Provider<CoreV1Api> coreV1ApiProvider;
    private final boolean enablePathStyleAccess;

    public static class S3Clients implements AutoCloseable {

        private static final Logger logger = LoggerFactory.getLogger(S3Clients.class);

        private final S3Client defaultClient;
        private final S3Client encryptingClient;

        public S3Clients(S3Client defaultClient) {
            this(defaultClient, null);
        }

        public S3Clients(S3Client defaultClient, S3Client encryptingClient) {
            this.defaultClient = defaultClient;
            this.encryptingClient = encryptingClient;
        }

        public boolean hasEncryptingClient() {
            return getEncryptingClient().isPresent();
        }

        public Optional<S3Client> getEncryptingClient() {
            return Optional.ofNullable(encryptingClient);
        }

        public S3Client getClient() {
            return Optional.ofNullable(encryptingClient).orElse(defaultClient);
        }

        public S3Client getNonEncryptingClient() {
            return defaultClient;
        }

        @Override
        public void close() throws Exception {
            tryCloseClient(encryptingClient);
            tryCloseClient(defaultClient);
        }

        private void tryCloseClient(S3Client s3Client) {
            if (s3Client == null)
                return;

            try {
                s3Client.close();
            } catch (Exception ex) {
                logger.warn("Unable to close S3 client: ", ex);
            }
        }
    }

    public S3ClientsFactory(Provider<CoreV1Api> coreV1ApiProvider) {
        this(coreV1ApiProvider, false);
    }

    public S3ClientsFactory(Provider<CoreV1Api> coreV1ApiProvider, boolean enablePathStyleAccess) {
        this.coreV1ApiProvider = coreV1ApiProvider;
        this.enablePathStyleAccess = enablePathStyleAccess;
    }

    public S3Clients build(final AbstractOperationRequest operationRequest) {
        return build(operationRequest, new S3ConfigurationResolver());
    }

    public S3Clients build(AbstractOperationRequest operationRequest, S3ConfigurationResolver configurationResolver) {
        final S3Configuration s3Conf = configurationResolver.resolveS3Configuration(coreV1ApiProvider, operationRequest);

        S3Client defaultS3Client = getDefaultS3Client(operationRequest, s3Conf);
        S3Client encryptingClient = null;

        if (s3Conf.awsKmsKeyId != null) {
            encryptingClient = getEncryptingClient(defaultS3Client, s3Conf);
        }

        return new S3Clients(defaultS3Client, encryptingClient);
    }

    private S3Client getEncryptingClient(S3Client wrappedClient, S3Configuration s3Conf) {
        return S3EncryptionClient.builder()
                                 .wrappedClient(wrappedClient)
                                 .kmsKeyId(s3Conf.awsKmsKeyId)
                                 .build();
    }

    private S3Client getDefaultS3Client(AbstractOperationRequest operationRequest, S3Configuration s3Conf) {
        S3ClientBuilder builder = S3Client.builder();

        if (s3Conf.awsRegion != null)
            builder.region(Region.of(s3Conf.awsRegion));

        if (enablePathStyleAccess)
            builder.forcePathStyle(enablePathStyleAccess);

        if (operationRequest.proxySettings != null && operationRequest.proxySettings.useProxy) {
            ApacheHttpClient.Builder clientBuilder = ApacheHttpClient.builder();

            ProxyConfiguration.Builder proxyBuilder = ProxyConfiguration.builder();

            try {
                URIBuilder uriBuilder = new URIBuilder();

                Optional.of(operationRequest.proxySettings.proxyPort)
                        .map(uriBuilder::setPort);
                Optional.of(operationRequest.proxySettings.proxyHost)
                        .map(uriBuilder::setHost);
                Optional.of(operationRequest.proxySettings.proxyProtocol)
                        .map(p -> uriBuilder.setScheme(p.toString()));

                proxyBuilder.endpoint(uriBuilder.build());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            Optional.ofNullable(operationRequest.proxySettings.proxyUsername)
                    .map(proxyBuilder::username);
            Optional.ofNullable(operationRequest.proxySettings.proxyPassword)
                    .map(proxyBuilder::password);

            clientBuilder.proxyConfiguration(proxyBuilder.build());

            builder.httpClientBuilder(clientBuilder);
        }

        if (S3ConfigurationResolver.isRunningInKubernetes()) {
            if (s3Conf.awsAccessKeyId != null && s3Conf.awsSecretKey != null) {
                builder.credentialsProvider(() -> new AwsCredentials()
                {
                    @Override
                    public String accessKeyId()
                    {
                        return s3Conf.awsAccessKeyId;
                    }

                    @Override
                    public String secretAccessKey()
                    {
                        return s3Conf.awsSecretKey;
                    }
                });
            }
        }

        return builder.build();
    }
}
