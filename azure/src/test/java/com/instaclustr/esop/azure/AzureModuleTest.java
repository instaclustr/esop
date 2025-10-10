package com.instaclustr.esop.azure;

import com.instaclustr.esop.azure.AzureModule.AzureModuleException;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

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
        setEnv("AZURE_STORAGE_ACCOUNT", "testAccountName");
        setEnv("AZURE_STORAGE_KEY", "testAccountKey");
        setEnv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testKey;EndpointSuffix=core.windows.net");

        ex = assertThrows(AzureModuleException.class, () -> {
            factory.build(new BackupOperationRequest());
        });
        assertEquals("Both AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY are set. Please set only one method of authentication.", ex.getMessage());

        // Clear connection string to test shared key credential authentication
        setEnv("AZURE_STORAGE_CONNECTION_STRING", "");
        assertNotNull(factory.build(new BackupOperationRequest()));

        // Clear shared key credentials to test connection string authentication
        setEnv("AZURE_STORAGE_ACCOUNT", "");
        setEnv("AZURE_STORAGE_KEY", "");
        setEnv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testKey;EndpointSuffix=core.windows.net");
        assertNotNull(factory.build(new BackupOperationRequest()));
    }

    @Test
    public void testSetEnv() throws Exception {
        setEnv("TEST_ENV_KEY1", "TEST_ENV_VALUE1");
        assertEquals("TEST_ENV_VALUE1", System.getenv("TEST_ENV_KEY1"));

        // Override the value

        setEnv("TEST_ENV_KEY2", "TEST_ENV_VALUE2");
        setEnv("TEST_ENV_KEY2", "TEST_ENV_VALUE3");
        assertEquals("TEST_ENV_VALUE3", System.getenv("TEST_ENV_KEY2"));
    }

    // Helper method to set environment variables for testing purposes
    private static void setEnv(String key, String value) throws Exception {
        Map<String, String> env = System.getenv();
        Class<?> cl = env.getClass();
        // It can potentially be broken in some jdk implementations if the name of the field in underlying UnmodifiableMap class is changed
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        writableEnv.put(key, value);
    }
}