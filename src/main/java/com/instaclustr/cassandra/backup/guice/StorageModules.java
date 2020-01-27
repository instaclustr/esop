package com.instaclustr.cassandra.backup.guice;

import com.google.inject.AbstractModule;
import com.instaclustr.cassandra.backup.aws.S3Module;
import com.instaclustr.cassandra.backup.azure.AzureModule;
import com.instaclustr.cassandra.backup.gcp.GCPModule;
import com.instaclustr.cassandra.backup.local.LocalFileModule;
import com.instaclustr.kubernetes.KubernetesApiModule;

public class StorageModules extends AbstractModule {

    @Override
    protected void configure() {
        install(new KubernetesApiModule());
        install(new S3Module());
        install(new AzureModule());
        install(new GCPModule());
        install(new LocalFileModule());
    }
}
