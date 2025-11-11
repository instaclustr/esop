package com.instaclustr.esop.impl.hash;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import static java.lang.String.format;

public class HashSpec {

    // Chunk size for reading files for hashing
    private static final int CHUNK_SIZE = 4096;

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

        default void doHashInternal(ReadableByteChannel ch, BiConsumer<ByteBuffer, Integer> consumer) throws Exception
        {
            ByteBuffer bb = ByteBuffer.allocate(CHUNK_SIZE);
            int bytesRead = 0;
            while ((bytesRead = ch.read(bb)) != -1) {
                bb.flip();
                consumer.accept(bb, bytesRead);
                bb.clear();
            }
        }

        String getHash(ReadableByteChannel ch) throws Exception;

        String getHash(byte[] digest) throws Exception;
    }

    private static class SHAHasher implements Hasher {
        private final String algorithm;

        public SHAHasher(String algorithm) {
            this.algorithm = algorithm;
        }

        @Override
        public String getHash(ReadableByteChannel ch) throws Exception
        {
            final MessageDigest digest = MessageDigest.getInstance(algorithm);
            doHashInternal(ch, (buffer, ignore) -> digest.update(buffer));
            return getHash(digest.digest());
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
        public String getHash(ReadableByteChannel ch) throws Exception {
            return null;
        }

        @Override
        public String getHash(byte[] digest) throws Exception {
            return null;
        }
    }

    public static class CRCHasher implements Hasher {
        @Override
        public String getHash(ReadableByteChannel ch) throws Exception
        {
            Checksum checksum = new CRC32();
            doHashInternal(ch, (buffer, ignored) -> checksum.update(buffer));
            return Long.toString(checksum.getValue());
        }

        @Override
        public String getHash(byte[] digest) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Wraps the xxHash64 algorithm. Used for fast hashing of large files as an alternative to SHA-256.
     */
    public static class XXHasher implements Hasher {

        @Override
        public String getHash(ReadableByteChannel ch) throws Exception {
            try (StreamingXXHash64 xxHash64 = XXHashFactory.fastestJavaInstance().newStreamingHash64(0)) {
                doHashInternal(ch, (buffer, bytesRead) -> xxHash64.update(buffer.array(), 0, bytesRead));
                return Long.toString(xxHash64.getValue());
            }
        }

        @Override
        public String getHash(final byte[] digest) throws Exception {
            // TODO do we actually need this?
            throw new UnsupportedOperationException();
        }
    }

    public enum HashAlgorithm {
        SHA_256("SHA-256", () -> new SHAHasher("SHA-256")),
        XXHASH64("xxHash64", () -> new XXHasher()),
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
                if (algorithm.name.equalsIgnoreCase(value)) {
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