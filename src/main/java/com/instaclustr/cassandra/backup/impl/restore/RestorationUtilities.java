package com.instaclustr.cassandra.backup.impl.restore;

import static java.util.stream.Collectors.toList;

import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.Manifest;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestorationUtilities {

    private static final Logger logger = LoggerFactory.getLogger(RestorationUtilities.class);

    public static Manifest downloadManifest(final RestoreOperationRequest request,
                                            final Restorer restorer,
                                            final String schemaVersion,
                                            final ObjectMapper objectMapper) throws Exception {

        final String manifestAsString = restorer.downloadNodeFileToString(Paths.get("manifests"),
                                                                          new ManifestFilteringPredicate(request, schemaVersion));

        return Manifest.read(manifestAsString, objectMapper);
    }

    public static List<ImportOperationRequest> buildImportRequests(final RestoreOperationRequest request, final DatabaseEntities entities) {
        return entities.getKeyspacesAndTables().entries().stream().map(entry -> request.importing.copy(entry.getKey(), entry.getValue())).collect(toList());
    }

    // predicates

    public static abstract class AbstractFilteringPredicate implements Predicate<String> {

        protected final RestoreOperationRequest request;
        protected final String currentSchemaVersion;

        public AbstractFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            this.request = request;
            this.currentSchemaVersion = currentSchemaVersion;
        }

        protected boolean filter(String toFilterOn) {
            if (request.exactSchemaVersion) {
                if (request.schemaVersion != null) {
                    return toFilterOn.contains(request.schemaVersion.toString());
                } else if (currentSchemaVersion != null) {
                    return toFilterOn.contains(currentSchemaVersion);
                }

                throw new IllegalStateException("exactSchemaVersion is required but there is not schemaVersion is request nor runtime Cassandra version!");
            } else {
                if (request.schemaVersion != null) {
                    return toFilterOn.contains(request.schemaVersion.toString());
                } else if (currentSchemaVersion != null) {
                    return toFilterOn.contains(currentSchemaVersion);
                } else {
                    return true;
                }
            }
        }
    }

    private static class ManifestFilteringPredicate extends AbstractFilteringPredicate {

        public ManifestFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            super(request, currentSchemaVersion);
        }

        @Override
        public boolean test(final String s) {
            String path = request.storageLocation.nodePath() + "/manifests/" + request.snapshotTag;
            return (s.contains(path) || s.startsWith(request.snapshotTag)) && filter(s);
        }
    }
}