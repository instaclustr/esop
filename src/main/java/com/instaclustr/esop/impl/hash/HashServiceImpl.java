package com.instaclustr.esop.impl.hash;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.inject.Inject;

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
                                                           path.toString(),
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

    private String getHash(final File file) throws IOException, NoSuchAlgorithmException {
        try (final FileInputStream fis = new FileInputStream(file)) {
            logger.info("Getting {} hash of {} ", hashSpec.algorithm.toString(), file.getAbsolutePath());
            final MessageDigest digest = MessageDigest.getInstance(hashSpec.algorithm.toString());

            // Create byte array to read data in chunks
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;

            // Read file data and update in message digest
            while ((bytesCount = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }

            byte[] bytes = digest.digest();

            final StringBuilder sb = new StringBuilder();

            //This bytes[] has bytes in decimal format, convert it to hexadecimal format
            for (final byte aByte : bytes) {
                sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        }
    }
}
