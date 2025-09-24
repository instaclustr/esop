package com.instaclustr.esop.impl.restore;

import java.nio.file.Paths;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.esop.impl.Manifest;

public class RestorationUtilities {

    public static Manifest downloadManifest(final RestoreOperationRequest request,
                                            final Restorer restorer,
                                            final String schemaVersion,
                                            final ObjectMapper objectMapper) throws Exception {

        final String manifestAsString = restorer.downloadManifest(Paths.get("manifests"),
                                                                  new ManifestFilteringPredicate(request, schemaVersion));

        return Manifest.read(manifestAsString, objectMapper);
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