package com.instaclustr.cassandra.backup.embedded.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.aws.S3Module;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.kubernetes.KubernetesService;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "k8sTest"
})
public class KubernetesS3BackupRestoreTest extends BaseS3BackupRestoreTest {

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws ApiException {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new S3Module());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws ApiException {
        destroy();
    }

    @Override
    protected void init() throws ApiException {
        System.setProperty("kubernetes.client", "true");

        kubernetesService.createSecret(SIDECAR_SECRET_NAME, new HashMap<String, String>() {{
            put("awssecretaccesskey", System.getProperty("awssecretaccesskey"));
            put("awsaccesskeyid", System.getProperty("awsaccesskeyid"));
        }});
    }

    @Override
    protected void destroy() throws ApiException {
        kubernetesService.deleteSecret("test-sidecar-secret");
        System.setProperty("kubernetes.client", "false");
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        backupOperationRequest.k8sBackupSecretName = SIDECAR_SECRET_NAME;
        return backupOperationRequest;
    }

    @Override
    public TransferManagerFactory getTransferManagerFactory() {
        return transferManagerFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments());
    }

    @Test
    @Ignore
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments());
    }
}
