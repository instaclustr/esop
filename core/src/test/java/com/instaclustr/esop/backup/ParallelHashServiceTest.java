package com.instaclustr.esop.backup;

import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.hash.HashService.HashingException;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.hash.ParallelHashService;
import com.instaclustr.esop.impl.hash.ParallelHashServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class ParallelHashServiceTest {
    private static final int THREAD_COUNT = 3;
    private static Path tempDir;
    private static final List<ManifestEntry> testManifestEntries = new ArrayList<>();

    @BeforeAll
    public static void setup() throws Exception {
        tempDir = Files.createTempDirectory("parallel-files-hash-test");
        for (int i = 0; i < THREAD_COUNT * 3; i++) {
            Path testFilePath = Files.createTempFile(tempDir, "test", "test");
            // writing file path as content to have some data
            Files.writeString(testFilePath, testFilePath.toString(), StandardCharsets.UTF_8);
        }
    }

    @BeforeEach
    public void prepareManifestEntries() throws Exception {
        testManifestEntries.clear();
        try (Stream<Path> stream = Files.list(tempDir)) {
            stream.forEach(path -> testManifestEntries.add(new ManifestEntry(null,
                                                                             path,
                                                                             ManifestEntry.Type.FILE,
                                                                             null,
                                                                             null)));
        }
    }

    @Test
    public void testHashAndPopulate() throws Exception {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            parallelHashService.hashAndPopulate(testManifestEntries);

            for (ManifestEntry entry : testManifestEntries) {
                // Hash should be populated for each ManifestEntry
                assertDoesNotThrow(() -> parallelHashService.verify(entry));
            }
        }
    }

    @Test
    public void testHashAndPopulateThrowsExceptionDuringExecution() throws Exception {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            List<ManifestEntry> faultyManifests = copyManifestsEntries();
            faultyManifests.forEach(m -> m.localFile = Path.of("non-existing-path"));
            assertThrowsExactly(HashingException.class, () -> parallelHashService.hashAndPopulate(faultyManifests));
        }
    }

    @Test
    public void testVerifyAll() throws Exception {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {
            // First, hash and populate the entries
            parallelHashService.hashAndPopulate(testManifestEntries);

            // Now verify all entries
            parallelHashService.verifyAll(testManifestEntries);
        }
    }

    @Test
    public void testVerifyAllExceptionDuringExecution() throws Exception {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {

            // First, hash and populate the entries
            parallelHashService.hashAndPopulate(testManifestEntries);
            // Create faulty manifest entries
            List<ManifestEntry> copiedEntries = copyManifestsEntries();
            for (ManifestEntry entry : copiedEntries) {
                entry.hash = "invalid-hash-value";
            }

            // Now verify all entries
            assertThrowsExactly(HashService.HashVerificationException.class, () -> {
                parallelHashService.verifyAll(copiedEntries);
            });
        }
    }

    @Test
    public void testSomeVerificationFails() throws Exception
    {
        try (ParallelHashService parallelHashService = new ParallelHashServiceImpl(new HashSpec(HashSpec.HashAlgorithm.XXHASH64), THREAD_COUNT)) {

            // First, hash and populate the entries
            parallelHashService.hashAndPopulate(testManifestEntries);
            // Create faulty manifest entries
            ManifestEntry copiedEntry = copyManifestsEntries().get(0);
            copiedEntry.hash = "invalid-hash-value";

            VerificationRunnable passes = new VerificationRunnable(parallelHashService, testManifestEntries.get(0), 5);
            VerificationRunnable fails = new VerificationRunnable(parallelHashService, copiedEntry, 3);

            // Now verify some will fail
            assertThrowsExactly(HashService.HashVerificationException.class, () -> {
                parallelHashService.verifyAllRunnables(List.of(passes, fails));
            });


            Assertions.assertTrue(fails.failed);
            Assertions.assertTrue(fails.wasExecuted);
            Assertions.assertFalse(fails.wasInterrputed);

            Assertions.assertFalse(passes.failed);
            Assertions.assertTrue(passes.wasExecuted);
            Assertions.assertTrue(passes.wasInterrputed);
        }
    }

    public static class VerificationRunnable implements Runnable {
        private final ParallelHashService service;
        private final ManifestEntry entry;
        private final int delay;

        public volatile boolean failed = false;
        public volatile boolean wasExecuted = false;
        public volatile boolean wasInterrputed = false;

        public VerificationRunnable(ParallelHashService service, ManifestEntry entry, int delay) {
            this.service = service;
            this.entry = entry;
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                wasExecuted = true;
                Thread.sleep(delay * 1000);
                try
                {
                    service.verify(entry);
                } catch (Throwable t) {
                    failed = true;
                    throw t;
                }
            }
            catch (InterruptedException ex) {
                wasInterrputed = true;
            }
        }
    }

    private static List<ManifestEntry> copyManifestsEntries() {
        return testManifestEntries.stream().map(
                e -> new ManifestEntry(
                        null,
                        e.localFile,
                        e.type,
                        e.hash,
                        null)
        ).collect(Collectors.toList());
    }
}
