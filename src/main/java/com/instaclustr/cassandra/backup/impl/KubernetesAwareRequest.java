package com.instaclustr.cassandra.backup.impl;

public interface KubernetesAwareRequest {

    String getNamespace();

    String getSecretName();
}
