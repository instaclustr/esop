package com.instaclustr.esop.azure;

import com.instaclustr.esop.azure.AzureModule.AzureModuleException;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.shared.WithEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BlobServiceClientFactoryTest {

    @Test
    public void testBuild() throws Exception {
        AzureModule.BlobServiceClientFactory factory = new AzureModule.BlobServiceClientFactory();

        // Test without any environment variables set
        AzureModuleException ex = assertThrows(AzureModuleException.class, () -> {
            factory.build(new BackupOperationRequest());
        });
        assertEquals("Azure credentials are not set. Please set either AZURE_STORAGE_CONNECTION_STRING or both AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY environment variables.", ex.getMessage());

        // Expect exception due to both authentication mechanisms being set
        try (final WithEnvironment withEnv = new WithEnvironment(
                "AZURE_STORAGE_ACCOUNT", "testAccountName",
                "AZURE_STORAGE_KEY", "testAccountKey",
                "AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testKey;EndpointSuffix=core.windows.net"
        )) {
            ex = assertThrows(AzureModuleException.class, () -> {
                factory.build(new BackupOperationRequest());
            });
            assertEquals("Both AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY are set. Please set only one method of authentication.", ex.getMessage());

            // Test with only AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY set
            withEnv.remove("AZURE_STORAGE_CONNECTION_STRING");
            assertNotNull(factory.build(new BackupOperationRequest()));

            // Test with only AZURE_STORAGE_CONNECTION_STRING set
            withEnv.remove("AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY");
            withEnv.set("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testKey;EndpointSuffix=core.windows.net");
            assertNotNull(factory.build(new BackupOperationRequest()));
        }
    }
}