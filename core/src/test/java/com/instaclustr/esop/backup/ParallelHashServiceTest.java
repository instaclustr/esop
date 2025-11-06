package com.instaclustr.esop.backup;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.hash.HashService.HashingException;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.hash.ParallelHashService;
import com.instaclustr.esop.impl.hash.ParallelHashServiceImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

public class ParallelHashServiceTest {
    private static final int THREAD_COUNT = 3;
    private static List<ManifestEntry> testManifestEntries = new ArrayList<>();

    @BeforeAll
    public static void setup() throws Exception {
        Path testDirPath = Files.createTempDirectory("paralell-files-hash-test");
        for (int i = 0; i < THREAD_COUNT * 3; i++) {
            Path testFilePath = Files.createTempFile(testDirPath, "test", "test");
            // writing file path as content to have some data
            Files.writeString(testFilePath, testFilePath.toString(), StandardCharsets.UTF_8);
            testManifestEntries.add(new ManifestEntry(
                    null,
                    testFilePath,
                    ManifestEntry.Type.FILE,
                    null,
                    null));
        }
    }

    @Test
    public void testHashAndPopulate() {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            parallelHashService.hashAndPopulate(testManifestEntries.stream()).join();

            for (ManifestEntry entry : testManifestEntries) {
                // Hash should be populated for each ManifestEntry
                assertDoesNotThrow(() -> parallelHashService.verify(entry));
            }
        } catch (Exception e) {
            fail("Should not throw exception during initialization of ParallelHashService", e);
        }
    }

    @Test
    public void testHashAndPopulateThrowsExceptionDuringExecution() {
        List<ManifestEntry> faultyManifests = testManifestEntries.stream().map(
                e -> new ManifestEntry(
                        null,
                        Path.of("non-existing-path"), // this will cause exception during hashing
                        e.type,
                        e.hash,
                        null)
        ).collect(Collectors.toList());

        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            assertThrowsExactly(HashingException.class, () -> parallelHashService.hashAndPopulate(faultyManifests.stream()).join());
        } catch (Exception e) {
            fail("Should not throw exception during initialization of ParallelHashService", e);
        }
    }
}
