package com.instaclustr.esop.azure;

import java.nio.file.Path;

import com.azure.storage.blob.specialized.BlockBlobClient;
import com.instaclustr.esop.impl.RemoteObjectReference;

public class AzureRemoteObjectReference extends RemoteObjectReference {

    public final BlockBlobClient blobClient;

    public AzureRemoteObjectReference(final Path objectKey,
                                      final String canonicalPath,
                                      final BlockBlobClient blobClient) {
        super(objectKey, canonicalPath);
        this.blobClient = blobClient;
    }

    public Path getObjectKey() {
        return objectKey;
    }
}
