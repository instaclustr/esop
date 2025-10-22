package com.instaclustr.esop.backup;

import java.io.File;
import java.nio.file.Files;

import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.hash.HashServiceImpl;
import com.instaclustr.esop.impl.hash.HashSpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HashServiceTest {

    private static File testFile;

    @BeforeAll
    public static void setup() throws Exception {
        testFile = File.createTempFile("hashingTest", ".tmp");
        Files.write(testFile.toPath(), "testdata".getBytes());
    }

    @AfterAll
    public static void teardown() throws Exception {
        if (testFile != null && testFile.exists()) {
            testFile.delete();
        }
    }

    @Test
    public void testHashing_DefaultAlgorithm() throws Exception {
        testHashing(new HashSpec());
    }

    @Test
    public void testHashing_SHA256() throws Exception {
        testHashing(new HashSpec(HashSpec.HashAlgorithm.SHA_256));
    }

    @Test
    public void testHashing_CRC32() throws Exception {
        testHashing(new HashSpec(HashSpec.HashAlgorithm.CRC));
    }

    @Test
    public void testHashing_xxHash64() throws Exception {
        testHashing(new HashSpec(HashSpec.HashAlgorithm.XXHASH64));
    }

    @Test
    public void testHashing_None() throws Exception {
        testHashing(new HashSpec(HashSpec.HashAlgorithm.NONE));
    }

    private void testHashing(HashSpec hashSpec) throws Exception {
        final HashService hashService = new HashServiceImpl(hashSpec);
        hashService.verify(testFile.toPath(), hashService.hash(testFile.toPath()));
    }
}
