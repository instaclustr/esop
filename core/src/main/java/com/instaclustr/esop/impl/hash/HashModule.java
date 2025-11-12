package com.instaclustr.esop.impl.hash;

import com.google.inject.AbstractModule;

public class HashModule extends AbstractModule {

    private final HashSpec hashSpec;
    private final int parallelHashingThreads;

    public HashModule(final HashSpec hashSpec, final int parallelHashingThreads) {
        this.hashSpec = hashSpec;
        this.parallelHashingThreads = parallelHashingThreads;
    }

    @Override
    protected void configure() {
        bind(HashSpec.class).toInstance(this.hashSpec);
        bind(HashService.class).toInstance(new HashServiceImpl(this.hashSpec));
        bind(ParallelHashService.class).toProvider(() -> new ParallelHashServiceImpl(this.hashSpec, this.parallelHashingThreads));
    }
}
