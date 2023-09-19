package com.instaclustr.esop.impl.list;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.Manifest.AllManifestsReport;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageInteractor;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.TableBuilder;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.esop.local.LocalFileRestorer;
import com.instaclustr.esop.topology.CassandraSimpleTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static com.instaclustr.esop.impl.list.ListOperationRequest.getForLocalListing;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;

public class ListOperation extends Operation<ListOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ListOperation.class);

    private final Map<String, RestorerFactory> restorerFactoryMap;
    private final ObjectMapper objectMapper;
    private final CassandraJMXService cassandraJMXService;

    @Inject
    public ListOperation(@Assisted final ListOperationRequest request,
                         final CassandraJMXService cassandraJMXService,
                         final Map<String, RestorerFactory> restorerFactoryMap,
                         final ObjectMapper objectMapper) {
        super(request);
        this.restorerFactoryMap = restorerFactoryMap;
        this.objectMapper = objectMapper;
        this.cassandraJMXService = cassandraJMXService;
    }

    @JsonCreator
    private ListOperation(@JsonProperty("type") final String type,
                          @JsonProperty("id") final UUID id,
                          @JsonProperty("creationTime") final Instant creationTime,
                          @JsonProperty("state") final State state,
                          @JsonProperty("errors") final List<Error> errors,
                          @JsonProperty("progress") final float progress,
                          @JsonProperty("startTime") final Instant startTime,
                          @JsonProperty("storageLocation") final StorageLocation storageLocation,
                          @JsonProperty("insecure") final boolean insecure,
                          @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                          @JsonProperty("proxySettings") final ProxySettings proxySettings,
                          @JsonProperty("retry") final RetrySpec retry,
                          @JsonProperty("json") final boolean json,
                          @JsonProperty("resolveNodes") final boolean resolveNodes,
                          @JsonProperty("humanUnits") final boolean humanUnits,
                          @JsonProperty("toFile") final String toFile,
                          @JsonProperty("simpleFormat") final boolean simpleFormat,
                          @JsonProperty("fromTimestamp") final Long fromTimestamp,
                          @JsonProperty("lastN") final Integer lastN,
                          @JsonProperty("skipDownload") final boolean skipDownload,
                          @JsonProperty("cacheDir") final Path cacheDir,
                          @JsonProperty("toRequest") final boolean toRequest,
                          @JsonProperty("response") final Manifest.AllManifestsReport response,
                          @JsonProperty("concurrentConnections") final Integer concurrentConnections) {
        super(type, id, creationTime, state, errors, progress, startTime, new ListOperationRequest(type,
                                                                                                   storageLocation,
                                                                                                   insecure,
                                                                                                   skipBucketVerification,
                                                                                                   proxySettings,
                                                                                                   retry,
                                                                                                   json,
                                                                                                   resolveNodes,
                                                                                                   humanUnits,
                                                                                                   toFile,
                                                                                                   simpleFormat,
                                                                                                   fromTimestamp,
                                                                                                   lastN,
                                                                                                   skipDownload,
                                                                                                   cacheDir,
                                                                                                   toRequest,
                                                                                                   concurrentConnections,
                                                                                                   response));
        this.restorerFactoryMap = null;
        this.objectMapper = null;
        this.cassandraJMXService = null;
    }

    @Override
    protected void run0() throws Exception {
        assert restorerFactoryMap != null;
        assert objectMapper != null;

        request.storageLocation.validate();

        Path localPath = request.cacheDir;

        if (!request.skipDownload && !Files.exists(localPath)) {
            FileUtils.createDirectory(localPath);
        }

        if (request.storageLocation.incompleteNodeLocation() && !request.resolveNodes) {
            throw new IllegalArgumentException("You have to specify full path to a node to list!");
        }

        if (request.resolveNodes) {
            assert cassandraJMXService != null;
            CassandraSimpleTopologyResult simpleTopology = new CassandraSimpleTopology(cassandraJMXService).act();
            request.storageLocation = StorageLocation.update(request.storageLocation,
                                                             simpleTopology.getClusterName(),
                                                             simpleTopology.getDc(),
                                                             simpleTopology.getHostId());
        }

        try (final StorageInteractor interactor = restorerFactoryMap.get(request.storageLocation.storageProvider).createListingInteractor(request)) {
            interactor.update(request.storageLocation, new LocalFileRestorer(getForLocalListing(request,
                                                                                                request.cacheDir,
                                                                                                request.storageLocation),
                                                                             objectMapper));

            final AllManifestsReport report = AllManifestsReport.report(interactor.listManifests());
            filterFromTimestamp(report, request.fromTimestamp);
            filterLastN(report, request.lastN);
            if (request.toRequest) {
                request.response = report;
            } else {
                try (final PrintStream ps = getOutputStream(request)) {
                    print(report, request, ps);
                }
            }
        } catch (final Exception ex) {
            logger.error("Unable to perform listing! - " + ex.getMessage(), ex);
            this.addError(Error.from(ex));
        }
    }

    private PrintStream getOutputStream(final ListOperationRequest request) throws Exception {
        if (request.toFile != null) {
            return new PrintStream(new FileOutputStream(request.toFile));
        } else {
            return System.out;
        }
    }


    private void filterFromTimestamp(final AllManifestsReport report, final long fromTimestamp) {
        // nothing to filter on
        if (fromTimestamp == Long.MAX_VALUE) {
            return;
        }
        final List<ManifestReport> filtered = report.getReports()
            .stream()
            .filter(mr -> mr.unixtimestamp <= fromTimestamp)
            .collect(toList());

        report.getReports().clear();
        report.getReports().addAll(filtered);
    }

    private void filterLastN(final AllManifestsReport report, final int lastN) {
        // nothing to filter on
        if (lastN < 1) {
            return;
        }

        // we get if from newest to oldest,
        // wanting "last n" means taking last n from the tail
        // so we reverse it first
        final List<ManifestReport> manifestReports = new ArrayList<>(report.getReports());
        reverse(manifestReports);

        final List<ManifestReport> filtered = manifestReports.stream().limit(lastN).collect(toList());
        // here we reverse it back so we have newest on top again
        reverse(filtered);
        report.getReports().clear();
        report.getReports().addAll(filtered);
    }

    private void print(final AllManifestsReport report, final ListOperationRequest request, final PrintStream ps) throws Exception {
        if (request.simpleFormat) {
            if (request.json) {
                printSimpleJson(report, ps);
            } else {
                printSimpleTable(report, ps);
            }
        } else {
            if (request.json) {
                printComplexJson(report, ps);
            } else {
                printComplexTable(report, ps);
            }
        }
    }

    private void printComplexJson(final AllManifestsReport report, final PrintStream ps) throws Exception {
        ps.println(objectMapper.writeValueAsString(report));
    }

    private void printSimpleJson(final AllManifestsReport report, final PrintStream ps) throws Exception {
        ps.println(objectMapper.writeValueAsString(report.reports.stream().map(mr -> mr.name).collect(toList())));
    }

    private void printSimpleTable(final AllManifestsReport report, final PrintStream ps) {
        final TableBuilder builder = new TableBuilder();

        for (final ManifestReport mr : report.reports) {
            builder.add(mr.name);
        }

        builder.printTo(ps);
    }

    private void printComplexTable(final AllManifestsReport report, final PrintStream ps) {
        final TableBuilder builder = new TableBuilder();

        builder.add("Timestamp", "Name", "Files", "Occupied space", "Reclaimable space");
        for (final ManifestReport mr : report.reports) {
            final String size = request.humanUnits ? humanReadableByteCountSI(mr.size) : Long.toString(mr.size);
            final String reclaimable = request.humanUnits ? humanReadableByteCountSI(mr.reclaimableSpace) : Long.toString(mr.reclaimableSpace);
            builder.add(mr.timestamp, mr.name, Integer.toString(mr.files), size, reclaimable);
        }

        final String totalSize = request.humanUnits ? humanReadableByteCountSI(report.totalSize) : Long.toString(report.totalSize);
        builder.add("", "", Integer.toString(report.totalFiles), totalSize, "");

        builder.printTo(ps);
    }

    private static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }
}
