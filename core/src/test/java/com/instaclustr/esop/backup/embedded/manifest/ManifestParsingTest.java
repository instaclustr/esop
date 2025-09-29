package com.instaclustr.esop.backup.embedded.manifest;

import java.util.stream.Stream;

import com.instaclustr.esop.impl.Manifest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static java.util.stream.Collectors.toList;

public class ManifestParsingTest
{

    @Test
    public void parseManifestWhenSnapshotIsSame()
    {
        String resolvedManifest = Manifest.parseLatestManifest(Stream.of("/manifests/snapshot1-some-uuid-1234567.json",
                                                                         "/manifests/snapshot1-some-uuid-1234805.json",
                                                                         "/manifests/snapshot1-some-uuid-1234700.json",
                                                                         "/manifests/snapshot1-some-uuid-1234450.json").collect(toList()));

        Assertions.assertEquals(resolvedManifest, "/manifests/snapshot1-some-uuid-1234805.json");
    }
}
