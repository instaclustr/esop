package com.instaclustr.esop.s3.v2;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.apache.internal.impl.ApacheSdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.encryption.s3.S3EncryptionClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class S3ClientsFactory {
    private static volatile Provider PROVIDER;

    public S3ClientsFactory() {
        if (PROVIDER == null) {
            Security.addProvider(new BouncyCastleProvider());
            PROVIDER = Security.getProvider("BC");
        }
    }

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
        return S3EncryptionClient.builder()
                                 .wrappedClient(wrappedClient)
                                 .kmsKeyId(kmsKeyId)
                                 .enableDelayedAuthenticationMode(true)
                                 .cryptoProvider(PROVIDER)
                                 .build();
    }

    private S3Client getDefaultS3Client(S3Configuration s3Conf, ProxySettings proxySettings) {
        S3ClientBuilder builder = S3Client.builder()
                                          .credentialsProvider(DefaultCredentialsProvider.create());
        if (s3Conf.awsRegion != null)
            builder.region(Region.of(s3Conf.awsRegion));

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        ProxyConfiguration proxyConfiguration = null;

        if (proxySettings != null && proxySettings.proxyHost != null && proxySettings.proxyPort != null) {
            ProxyConfiguration.Builder configuration = ProxyConfiguration.builder();

            try {
                String proxyProtocol = proxySettings.proxyProtocol == null ? "https" : proxySettings.proxyProtocol.toString();
                String proxyPort = proxySettings.proxyPort.toString();
                URI uri = URI.create(String.format("%s://%s:%s", proxyProtocol, proxySettings.proxyHost, proxyPort));
                configuration.endpoint(uri);
            } catch (Throwable t) {
                throw new IllegalArgumentException("Unable to set proxy", t);
            }

            if (proxySettings.proxyUsername != null && proxySettings.proxyPassword != null) {
                configuration.username(proxySettings.proxyUsername);
                configuration.password(proxySettings.proxyPassword);
            }

            proxyConfiguration = configuration.build();
        }

        httpClientBuilder = httpClientBuilder.maxConnections(1000);

        if (System.getenv("AWS_CA_BUNDLE") != null)
        {
            SSLContext sslContext = createSslContextFromPem(Paths.get(System.getenv("AWS_CA_BUNDLE")));
            httpClientBuilder.socketFactory(new SSLConnectionSocketFactory(sslContext));
        }


        SdkHttpClient sdkHttpClient = null;

        if (proxyConfiguration == null) {
            sdkHttpClient = httpClientBuilder.build();
        } else {
            sdkHttpClient = httpClientBuilder.proxyConfiguration(proxyConfiguration).build();
        }

        return builder.httpClient(sdkHttpClient).build();
    }

    public static SSLContext createSslContextFromPem(Path pemPath) throws RuntimeException {
        try
        {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");

            List<X509Certificate> certs = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(pemPath)) {
                StringBuilder certBlock = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    certBlock.append(line).append('\n');
                    if (line.contains("END CERTIFICATE")) {
                        try (InputStream certStream = new ByteArrayInputStream(certBlock.toString().getBytes(StandardCharsets.UTF_8))) {
                            certs.add((X509Certificate) cf.generateCertificate(certStream));
                        }
                        certBlock.setLength(0);
                    }
                }
            }

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null);
            int i = 0;
            for (X509Certificate cert : certs) {
                ks.setCertificateEntry("custom-ca-" + i++, cert);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }
}
