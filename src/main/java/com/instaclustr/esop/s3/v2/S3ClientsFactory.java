package com.instaclustr.esop.s3.v2;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.Protocol;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.encryption.s3.S3EncryptionClient;

public class S3ClientsFactory {

    public static class S3Clients implements AutoCloseable {

        private static final Logger logger = LoggerFactory.getLogger(S3Clients.class);

        private final S3Client defaultClient;
        private final S3Client encryptingClient;
        private final String kmsKeyIdOfEncryptedClient;

        public S3Clients(S3Client defaultClient) {
            this(defaultClient, null, null);
        }

        public S3Clients(S3Client defaultClient, S3Client encryptingClient, String kmsKeyIdOfEncryptedClient) {
            this.defaultClient = defaultClient;
            this.encryptingClient = encryptingClient;
            this.kmsKeyIdOfEncryptedClient = kmsKeyIdOfEncryptedClient;
        }

        public boolean hasEncryptingClient() {
            return getEncryptingClient().isPresent();
        }

        public Optional<S3Client> getEncryptingClient() {
            return Optional.ofNullable(encryptingClient);
        }

        public Optional<String> getKMSKeyOfEncryptedClient() {
            return Optional.ofNullable(kmsKeyIdOfEncryptedClient);
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

    public S3Clients build(S3ConfigurationResolver configurationResolver) {
        final S3Configuration s3Conf = configurationResolver.resolveS3ConfigurationFromEnvProperties();
        final ProxySettings proxySettings = Optional.ofNullable(configurationResolver.request).map(r -> r.proxySettings).orElse(null);

        S3Client defaultS3Client = getDefaultS3Client(s3Conf, proxySettings);
        S3Client encryptingClient = null;

        if (s3Conf.awsKmsKeyId != null) {
            encryptingClient = getEncryptingClient(defaultS3Client, s3Conf);
        }

        return new S3Clients(defaultS3Client, encryptingClient, s3Conf.awsKmsKeyId);
    }

    public S3Client getEncryptingClient(S3Client wrappedClient, S3Configuration s3Conf) {
        return getEncryptingClient(wrappedClient, s3Conf.awsKmsKeyId);
    }

    public S3Client getEncryptingClient(S3Client wrappedClient, String kmsKeyId) {
        return S3EncryptionClient.builder().wrappedClient(wrappedClient).kmsKeyId(kmsKeyId).build();
    }

    private S3Client getDefaultS3Client(S3Configuration s3Conf, ProxySettings proxySettings) {
        S3ClientBuilder builder = S3Client.builder()
                                          .credentialsProvider(DefaultCredentialsProvider.create());
        if (s3Conf.awsRegion != null)
            builder.region(Region.of(s3Conf.awsRegion));

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        if (proxySettings != null) {
            ProxyConfiguration.Builder configuration = ProxyConfiguration.builder();
            if (proxySettings.proxyHost != null && proxySettings.proxyPort != null) {
                try {
                    String proxyProtocol = proxySettings.proxyProtocol == null ? "https" : proxySettings.proxyProtocol.toString();
                    String proxyPort = proxySettings.proxyPort.toString();
                    URI uri = URI.create(String.format("%s://%s:%s", proxyProtocol, proxySettings.proxyHost, proxyPort));
                    configuration.endpoint(uri);
                } catch (Throwable t) {
                    throw new IllegalArgumentException("Unable to set proxy", t);
                }
            }
            if (proxySettings.proxyUsername != null && proxySettings.proxyPassword != null) {
                configuration.username(proxySettings.proxyUsername);
                configuration.password(proxySettings.proxyPassword);
            }
        }

        builder.httpClient(httpClientBuilder.build());

        return builder.build();
    }
}
