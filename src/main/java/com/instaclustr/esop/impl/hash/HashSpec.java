package com.instaclustr.esop.impl.hash;

import java.util.Arrays;

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
        description = "Algorithm to use for hashing of SSTables and files to upload / download.",
        defaultValue = "SHA-256",
        converter = HashAlgorithmConverter.class)
    public HashAlgorithm algorithm;

    private static class HashAlgorithmConverter implements CommandLine.ITypeConverter<HashAlgorithm> {

        @Override
        public HashAlgorithm convert(final String value) {
            return HashAlgorithm.parse(value);
        }
    }

    public enum HashAlgorithm {
        SHA_256("SHA-256");

        private static final Logger logger = LoggerFactory.getLogger(HashAlgorithm.class);
        public static final HashAlgorithm DEFAULT_ALGORITHM = HashAlgorithm.SHA_256;

        private final String name;

        HashAlgorithm(final String name) {
            this.name = name;
        }

        public String toString() {
            return name;
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
