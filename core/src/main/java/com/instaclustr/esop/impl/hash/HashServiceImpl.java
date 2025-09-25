package com.instaclustr.esop.impl.hash;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.ManifestEntry;

import static java.lang.String.format;

public class HashServiceImpl implements HashService {

    private static final Logger logger = LoggerFactory.getLogger(HashServiceImpl.class);

    private final HashSpec hashSpec;

    @Inject
    public HashServiceImpl(final HashSpec hashSpec) {
        this.hashSpec = hashSpec;
    }

    @Override
    public String hash(final Path path) throws HashingException {
        try {
            if (path == null) {
                throw new HashingException("file to get a hash from is null!");
            }
            return getHash(path.toAbsolutePath().toFile());
        } catch (final HashingException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new HashingException(format("Unable to get hash for file '%s'.",
                                              path == null ? null : path.toString()),
                                       ex);
        }
    }

    @Override
    public String hash(final ManifestEntry entry) throws HashingException {
        return hash(entry.localFile);
    }

    @Override
    public void verify(final ManifestEntry entry) throws HashVerificationException {
        verify(entry.localFile, entry.hash);
    }

    @Override
    public void verify(final Path path, final String expectedHash) throws HashVerificationException {

        if (hashSpec.algorithm == HashSpec.HashAlgorithm.NONE) {
            return;
        }

        try {
            if (path == null) {
                throw new HashVerificationException("file to get a hash for is null!");
            }

            if (expectedHash == null) {
                throw new HashVerificationException("hash to expect is null!");
            }

            final String hashOfFile = getHash(path.toAbsolutePath().toFile());

            if (!hashOfFile.equals(expectedHash)) {
                throw new HashVerificationException(format("hash of %s (%s) does not match with expected hash %s",
                                                           path,
                                                           hashOfFile,
                                                           expectedHash));
            }
        } catch (final HashVerificationException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new HashVerificationException(format("Unable to get a hash for path %s, reason: %s",
                                                       path == null ? null : path.toString(),
                                                       ex.getMessage()),
                                                ex);
        }
    }

    private String getHash(final File file) throws Exception
    {
        if (hashSpec.algorithm == HashSpec.HashAlgorithm.NONE)
            return null;
        try (final InputStream is = new FileInputStream(file)) {
            logger.info("Getting {} hash of {} ", hashSpec.algorithm.toString(), file.getAbsolutePath());
            return hashSpec.algorithm.getHasher().getHash(is);
        }
    }
}
