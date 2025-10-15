package com.instaclustr.esop.azure;

import com.instaclustr.esop.azure.AzureModule.AzureModuleException;
import com.instaclustr.esop.azure.AzureModule.BlobServiceClientFactory;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.shared.WithEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BlobServiceClientFactoryTest {

    private final static String TEST_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testKey;EndpointSuffix=core.windows.net";

    @Test
    public void testBuild() throws Exception {
        BlobServiceClientFactory factory = new BlobServiceClientFactory();

        // Test without any environment variables set
        AzureModuleException ex = assertThrows(AzureModuleException.class, () -> {
            factory.build(new BackupOperationRequest());
        });
        assertEquals(BlobServiceClientFactory.NO_AZURE_CREDENTIALS_ERROR_MESSAGE, ex.getMessage());

        // Expect exception due to both authentication mechanisms being set
        try (final WithEnvironment withEnv = new WithEnvironment(
                "AZURE_STORAGE_ACCOUNT", "testAccountName",
                "AZURE_STORAGE_KEY", "testAccountKey",
                "AZURE_STORAGE_CONNECTION_STRING", TEST_CONNECTION_STRING
        )) {
            ex = assertThrows(AzureModuleException.class, () -> {
                factory.build(new BackupOperationRequest());
            });
            assertEquals(BlobServiceClientFactory.BOTH_CREDENTIALS_SET_ERROR_MESSAGE, ex.getMessage());

            // Test with only AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY set
            withEnv.remove("AZURE_STORAGE_CONNECTION_STRING");
            assertNotNull(factory.build(new BackupOperationRequest()));

            // Test with only AZURE_STORAGE_CONNECTION_STRING set
            withEnv.remove("AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY");
            withEnv.set("AZURE_STORAGE_CONNECTION_STRING", TEST_CONNECTION_STRING);
            assertNotNull(factory.build(new BackupOperationRequest()));
        }
    }
}