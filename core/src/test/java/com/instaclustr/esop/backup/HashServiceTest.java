package com.instaclustr.esop.backup;

import java.io.File;
import java.nio.file.Files;

import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.hash.HashServiceImpl;
import com.instaclustr.esop.impl.hash.HashSpec;
import org.junit.Test;

public class HashServiceTest {

    @Test
    public void testHashing() throws Exception {
        final File f = File.createTempFile("hashingTest", ".tmp");
        Files.write(f.toPath(), "".getBytes());
        final HashService hashService = new HashServiceImpl(new HashSpec());
        hashService.verify(f.toPath(), hashService.hash(f.toPath()));
    }

}
