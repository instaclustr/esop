package com.instaclustr.esop.guice;

import com.google.inject.AbstractModule;
import com.instaclustr.esop.azure.AzureModule;
import com.instaclustr.esop.gcp.GCPModule;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.esop.s3.aws_v2.S3V2Module;

public class StorageModules extends AbstractModule
{
    @Override
    protected void configure()
    {
        install(new AzureModule());
        install(new GCPModule());
        install(new LocalFileModule());
        install(new S3V2Module());
    }
}
