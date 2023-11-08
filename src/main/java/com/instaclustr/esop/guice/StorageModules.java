package com.instaclustr.esop.guice;

import com.google.inject.AbstractModule;
import com.instaclustr.esop.azure.AzureModule;
import com.instaclustr.esop.gcp.GCPModule;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.esop.s3.aws.S3Module;
import com.instaclustr.esop.s3.aws_v2.S3V2Module;
import com.instaclustr.esop.s3.ceph.CephModule;
import com.instaclustr.esop.s3.minio.MinioModule;
import com.instaclustr.esop.s3.oracle.OracleModule;
import com.instaclustr.kubernetes.KubernetesApiModule;

public class StorageModules extends AbstractModule
{
    @Override
    protected void configure()
    {
        install(new KubernetesApiModule());
        install(new AzureModule());
        install(new GCPModule());
        install(new LocalFileModule());
        install(new OracleModule());
        install(new MinioModule());
        install(new CephModule());
        install(new S3Module());
        install(new S3V2Module());
    }
}
