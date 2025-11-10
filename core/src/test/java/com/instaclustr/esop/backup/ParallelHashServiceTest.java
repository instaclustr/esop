package com.instaclustr.esop.backup;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.hash.HashService.HashingException;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.hash.ParallelHashService;
import com.instaclustr.esop.impl.hash.ParallelHashServiceImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ParallelHashServiceTest {
    private static final int THREAD_COUNT = 3;
    private static Path tempDir;
    private static List<ManifestEntry> testManifestEntries = new ArrayList<>();

    @BeforeAll
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("paralell-files-hash-test");
        for (int i = 0; i < THREAD_COUNT * 3; i++) {
            Path testFilePath = Files.createTempFile(tempDir, "test", "test");
            // writing file path as content to have some data
            Files.writeString(testFilePath, testFilePath.toString(), StandardCharsets.UTF_8);
        }
    }

    @BeforeEach
    public void prepareManifestEntries() throws Exception {
        testManifestEntries.clear();
        Files.list(tempDir).forEach(path -> {
            testManifestEntries.add(new ManifestEntry(
                    null,
                    path,
                    ManifestEntry.Type.FILE,
                    null,
                    null));
        });
    }

    @Test
    public void testHashAndPopulate() {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            parallelHashService.hashAndPopulate(testManifestEntries).join();

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
        List<ManifestEntry> faultyManifests = faultyManifestEntriesOf(testManifestEntries);

        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            assertThrowsExactly(HashingException.class, () -> parallelHashService.hashAndPopulate(faultyManifests).join());
        } catch (Exception e) {
            fail("Should not throw exception during initialization of ParallelHashService", e);
        }
    }

    @Test
    public void testVerifyAll() {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            // First, hash and populate the entries
            parallelHashService.hashAndPopulate(testManifestEntries).join();

            // Now verify all entries
            parallelHashService.verifyAll(testManifestEntries);
        } catch (Exception e) {
            fail("Should not throw", e);
        }
    }

    @Test
    public void testVerifyAllExceptionDuringExecution() {
        ForkJoinPool forkJoinPool = new ForkJoinPool(THREAD_COUNT);

        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), forkJoinPool)) {

            // First, hash and populate the entries
            parallelHashService.hashAndPopulate(testManifestEntries).join();
            // Create faulty manifest entries
            List<ManifestEntry> faultyManifests = faultyManifestEntriesOf(testManifestEntries);

            // Now verify all entries
            assertThrowsExactly(HashService.HashVerificationException.class, () -> {
                parallelHashService.verifyAll(faultyManifests);
            });

            assertTrue(forkJoinPool.getQueuedTaskCount() < 1, "ForkJoinPool should have no queued tasks after verification failure");
        }
        catch (Exception e) {
            fail("Should not throw exception during initialization of ParallelHashService", e);
        }
    }

    private static List<ManifestEntry> faultyManifestEntriesOf(List<ManifestEntry> originalEntries) {
        return originalEntries.stream().map(
                e -> new ManifestEntry(
                        null,
                        Path.of("non-existing-path"), // this will cause exception during verification
                        e.type,
                        e.hash,
                        null)
        ).collect(Collectors.toList());
    }
}
