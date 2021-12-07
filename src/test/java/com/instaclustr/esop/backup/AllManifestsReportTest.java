package com.instaclustr.esop.backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Collections;

public class AllManifestsReportTest {

    @Test
    public void testSerialization() {
        Manifest.AllManifestsReport report = new Manifest.AllManifestsReport();
        report.totalFiles = 10;
        report.totalSize = 500;
        report.totalManifests = 10;

        Manifest.ManifestReporter.ManifestReport manifestReport = new Manifest.ManifestReporter.ManifestReport();
        manifestReport.name = "123";
        manifestReport.reclaimableSpace = 100;
        manifestReport.size = 10;
        manifestReport.files = 3;
        manifestReport.unixtimestamp = 124L;
        manifestReport.removableEntries = Collections.singletonList("123");
        manifestReport.manifest = new ManifestEntry(Paths.get("somekey"),
                                                    Paths.get("localFile"),
                                                    ManifestEntry.Type.MANIFEST_FILE,
                                                    100,
                                                    new KeyspaceTable("ks1", "tb1"),
                                                    "123");

        report.reports = Collections.singletonList(manifestReport);

        ObjectMapper mapper = new ObjectMapper();
        try {
            String s = mapper.writeValueAsString(report);
            Manifest.AllManifestsReport report1 = mapper.readValue(s, Manifest.AllManifestsReport.class);
            System.out.println(report1);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
