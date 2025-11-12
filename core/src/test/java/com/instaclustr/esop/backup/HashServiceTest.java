    package com.instaclustr.esop.backup;

    import java.net.URL;
    import java.nio.file.Path;

    import com.instaclustr.esop.impl.hash.HashService;
    import com.instaclustr.esop.impl.hash.HashServiceImpl;
    import com.instaclustr.esop.impl.hash.HashSpec;
    import org.junit.jupiter.api.BeforeAll;
    import org.junit.jupiter.api.Test;

    import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
    import static org.junit.jupiter.api.Assertions.assertNotNull;

    public class HashServiceTest {

        private static final String HASH_TEST_FILE_LOCATION = "hash/hash-test-file-12K";
        private static Path testFilePath;

        @BeforeAll
        public static void setup() throws Exception {
            URL resourceUrl = HashServiceTest.class.getClassLoader().getResource(HASH_TEST_FILE_LOCATION);
            assertNotNull(resourceUrl, "Test file resource not found: " + HASH_TEST_FILE_LOCATION);
            testFilePath = Path.of(resourceUrl.toURI().getPath()).toAbsolutePath();
        }

        private static final String EXPECTED_SHA256_HASH = "01d8740e8d0b16d0468324a7952f483c9f360529966b29b6cd1bf81ca2988c5b";
        private static final String EXPECTED_XXHASH64_HASH = "ce26d6e69ac4d755";

        @Test
        public void testHashing_DefaultAlgorithm() {
            testHashing(new HashSpec(), EXPECTED_SHA256_HASH);
        }

        @Test
        public void testHashing_SHA256() {
            testHashing(new HashSpec(HashSpec.HashAlgorithm.SHA_256), EXPECTED_SHA256_HASH);
        }

        @Test
        public void testHashing_xxHash64() {
            testHashing(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), EXPECTED_XXHASH64_HASH);
        }

        @Test
        public void testHashing_None() {
            testHashing(new HashSpec(HashSpec.HashAlgorithm.NONE), null);
        }

        private void testHashing(HashSpec hashSpec, String expectedHash) {
            final HashService hashService = new HashServiceImpl(hashSpec);
            assertDoesNotThrow(() -> hashService.verify(testFilePath, expectedHash));
        }
    }
