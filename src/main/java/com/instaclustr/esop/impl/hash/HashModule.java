package com.instaclustr.esop.impl.hash;

import com.google.inject.AbstractModule;

public class HashModule extends AbstractModule {

    private final HashSpec hashSpec;

    public HashModule(final HashSpec hashSpec) {
        this.hashSpec = hashSpec;
    }

    @Override
    protected void configure() {
        bind(HashSpec.class).toInstance(this.hashSpec);
        bind(HashService.class).toInstance(new HashServiceImpl(this.hashSpec));
    }
}
