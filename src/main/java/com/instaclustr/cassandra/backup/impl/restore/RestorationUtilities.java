package com.instaclustr.cassandra.backup.impl.restore;

import static java.util.stream.Collectors.toList;

import java.nio.file.Paths;
import java.util.List;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.Manifest;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;

public class RestorationUtilities {

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

    public static abstract class AbstractFilteringPredicate implements Predicate<String> {

        protected final RestoreOperationRequest request;
        protected final String currentSchemaVersion;

        protected String path;

        public AbstractFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            this.request = request;
            this.currentSchemaVersion = currentSchemaVersion;
        }

        protected boolean filter(String toFilterOn) {

            if (!toFilterOn.contains(path)) {
                return false;
            }

            if (request.exactSchemaVersion) {
                if (request.schemaVersion != null) {
                    return toFilterOn.contains(path + "-" + request.schemaVersion);
                } else if (currentSchemaVersion != null) {
                    return toFilterOn.contains(path + "-" + currentSchemaVersion);
                }
            }

            return true;
        }
    }

    private static class ManifestFilteringPredicate extends AbstractFilteringPredicate {

        public ManifestFilteringPredicate(final RestoreOperationRequest request, final String currentSchemaVersion) {
            super(request, currentSchemaVersion);
        }

        @Override
        public boolean test(final String s) {
            path = request.storageLocation.nodePath() + "/manifests/" + request.snapshotTag;
            return filter(s);
        }
    }
}