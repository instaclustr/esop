package com.instaclustr.esop.backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.list.ListOperation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class AllManifestsReportTest {

    @Test
    public void testSerialization() throws Exception {
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
                                                    "123",
                                                    null);

        report.reports = Collections.singletonList(manifestReport);



        ObjectMapper mapper = new ObjectMapper();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ListOperation.print(mapper, report, getRequest(false, false), new PrintStream(baos));
        String result = baos.toString();

        String output = "Timestamp Name Files Occupied space Reclaimable space\n" +
                        "          123  3     10             100              \n" +
                        "               10    500                             \n";

        assertEquals(output, result);

        //////

        baos = new ByteArrayOutputStream();
        ListOperation.print(mapper, report, getRequest(true, false), new PrintStream(baos));
        result = baos.toString();

        output = "Timestamp Name Files Occupied space Reclaimable space\n" +
                 "          123  3     10 B           100 B            \n" +
                 "               10    500 B                           \n";

        assertEquals(output, result);

        //////

        baos = new ByteArrayOutputStream();
        ListOperation.print(mapper, report, getRequest(false, true), new PrintStream(baos));
        result = baos.toString();

        output = "{\n" +
                "  \"totalSize\" : 500,\n" +
                "  \"totalFiles\" : 10,\n" +
                "  \"totalManifests\" : 10,\n" +
                "  \"reports\" : [ {\n" +
                "    \"files\" : 3,\n" +
                "    \"size\" : 10,\n" +
                "    \"name\" : \"123\",\n" +
                "    \"reclaimableSpace\" : 100,\n" +
                "    \"removableEntries\" : [ \"123\" ],\n" +
                "    \"timestamp\" : null,\n" +
                "    \"manifest\" : {\n" +
                "      \"objectKey\" : \"somekey\",\n" +
                "      \"type\" : \"MANIFEST_FILE\",\n" +
                "      \"size\" : 100,\n" +
                "      \"hash\" : \"123\"\n" +
                "    },\n" +
                "    \"unixtimestamp\" : 124\n" +
                "  } ]\n" +
                "}\n";

        assertEquals(output, result);

        /////

        baos = new ByteArrayOutputStream();
        ListOperation.print(mapper, report, getRequest(true, true), new PrintStream(baos));
        result = baos.toString();

        output = "{\n" +
                "  \"totalSize\" : \"500 B\",\n" +
                "  \"totalFiles\" : 10,\n" +
                "  \"totalManifests\" : 10,\n" +
                "  \"reports\" : [ {\n" +
                "    \"files\" : 3,\n" +
                "    \"size\" : \"10 B\",\n" +
                "    \"name\" : \"123\",\n" +
                "    \"reclaimableSpace\" : \"100 B\",\n" +
                "    \"removableEntries\" : [ \"123\" ],\n" +
                "    \"timestamp\" : null,\n" +
                "    \"manifest\" : {\n" +
                "      \"objectKey\" : \"somekey\",\n" +
                "      \"type\" : \"MANIFEST_FILE\",\n" +
                "      \"size\" : \"100 B\",\n" +
                "      \"hash\" : \"123\"\n" +
                "    },\n" +
                "    \"unixtimestamp\" : 124\n" +
                "  } ]\n" +
                "}\n";

        assertEquals(output, result);
    }

    private ListOperationRequest getRequest(boolean humanUnits, boolean json) {
        ListOperationRequest request = new ListOperationRequest();
        request.humanUnits = humanUnits;
        request.json = json;
        return request;
    }
}
