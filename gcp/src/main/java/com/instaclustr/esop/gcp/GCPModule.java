package com.instaclustr.esop.gcp;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.esop.SPIModule;
import com.instaclustr.esop.guice.BackupRestoreBindings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class GCPModule extends AbstractModule implements SPIModule
{
    @Override
    protected void configure() {
        BackupRestoreBindings.installBindings(binder(),
                                              "gcp",
                                              GCPRestorer.class,
                                              GCPBackuper.class,
                                              GCPBucketService.class);
    }

    @Provides
    @Singleton
    GoogleStorageFactory provideGoogleStorageFactory() {
        return new GoogleStorageFactory();
    }

    @Override
    public AbstractModule getGuiceModule()
    {
        return this;
    }

    public static class GoogleStorageFactory {

        private static final Logger logger = LoggerFactory.getLogger(GoogleStorageFactory.class);

        public Storage build() {
            return resolveStorageFromEnvProperties();
        }

        private Storage resolveStorageFromEnvProperties() {
            String googleAppCredentialsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

            if (googleAppCredentialsPath == null) {
                logger.warn("GOOGLE_APPLICATION_CREDENTIALS environment property was not set, going to try system property google.application.credentials");

                googleAppCredentialsPath = System.getProperty("google.application.credentials");

                if (googleAppCredentialsPath == null) {
                    throw new GCPModuleException("google.application.credentials system property was not set.");
                }
            }

            if (!Files.exists(Paths.get(googleAppCredentialsPath))) {
                throw new GCPModuleException(format("GCP credentials file %s does not exist!", googleAppCredentialsPath));
            }

            GoogleCredentials credentials = resolveGoogleCredentialsFromFile(googleAppCredentialsPath);
            return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        }

        private GoogleCredentials resolveGoogleCredentialsFromFile(String googleAppCredentialsPath) {
            try (InputStream is = new FileInputStream(googleAppCredentialsPath)) {
                return GoogleCredentials.fromStream(is);
            }
            catch (Exception ex) {
                throw new RuntimeException("Unable to read credentials from " + googleAppCredentialsPath);
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
