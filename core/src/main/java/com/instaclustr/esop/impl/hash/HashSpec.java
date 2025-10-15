package com.instaclustr.esop.impl.hash;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import static java.lang.String.format;

public class HashSpec {

    public HashSpec(final HashAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    public HashSpec() {
        this.algorithm = HashAlgorithm.DEFAULT_ALGORITHM;
    }

    @Option(names = {"--hash-algorithm"},
        description = "Algorithm to use for hashing of SSTables and files to upload / download. For skipping, use NONE.",
        defaultValue = "SHA-256",
        converter = HashAlgorithmConverter.class)
    public HashAlgorithm algorithm;

    private static class HashAlgorithmConverter implements CommandLine.ITypeConverter<HashAlgorithm> {

        @Override
        public HashAlgorithm convert(final String value) {
            return HashAlgorithm.parse(value);
        }
    }

    public interface Hasher {

        String getHash(InputStream is) throws Exception;

        String getHash(byte[] digest) throws Exception;
    }

    private static class SHAHasher implements Hasher {
        private final String algorithm;
        public SHAHasher(String algorithm) {
            this.algorithm = algorithm;
        }

        @Override
        public String getHash(InputStream is) throws Exception
        {
            final MessageDigest digest = MessageDigest.getInstance(algorithm);

            // Create byte array to read data in chunks
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;

            // Read file data and update in message digest
            while ((bytesCount = is.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }

            byte[] bytes = digest.digest();

            return getHash(bytes);
        }

        @Override
        public String getHash(byte[] digest) throws Exception {
            final StringBuilder sb = new StringBuilder();

            for (final byte aByte : digest) {
                sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        }
    }

    public static class NoOp implements Hasher {
        @Override
        public String getHash(InputStream is) throws Exception {
            return null;
        }

        @Override
        public String getHash(byte[] digest) throws Exception {
            return null;
        }
    }

    public static class CRCHasher implements Hasher {
        @Override
        public String getHash(InputStream is) throws Exception
        {
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;

            Checksum checksum = new CRC32();

            while ((bytesCount = is.read(byteArray)) != -1) {
                checksum.update(byteArray, 0, bytesCount);
            }

            return Long.toString(checksum.getValue());
        }

        @Override
        public String getHash(byte[] digest) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    public enum HashAlgorithm {
        SHA_256("SHA-256", () -> new SHAHasher("SHA-256")),
        CRC("CRC", () -> new CRCHasher()),
        NONE("NONE", () -> new NoOp());

        private static final Logger logger = LoggerFactory.getLogger(HashAlgorithm.class);
        public static final HashAlgorithm DEFAULT_ALGORITHM = HashAlgorithm.SHA_256;

        private final String name;
        private final Supplier<Hasher> hasherSupplier;

        HashAlgorithm(final String name, final Supplier<Hasher> hasherSupplier) {
            this.name = name;
            this.hasherSupplier = hasherSupplier;
        }

        public String toString() {
            return name;
        }

        public Hasher getHasher() {
            return hasherSupplier.get();
        }

        public static HashAlgorithm parse(final String value) {
            if (value == null || value.trim().isEmpty()) {
                return HashAlgorithm.DEFAULT_ALGORITHM;
            }

            for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
                if (algorithm.name.equals(value)) {
                    return algorithm;
                }
            }

            logger.info(format("Unable to parse hash algorithm for value '%s', possible algorithms: %s, returning default algorithm %s",
                               value,
                               Arrays.toString(HashAlgorithm.values()),
                               HashAlgorithm.DEFAULT_ALGORITHM));

            return HashAlgorithm.DEFAULT_ALGORITHM;
        }
    }
}
