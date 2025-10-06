package com.instaclustr.operations;

public abstract class FunctionWithEx<U, T> {

    public abstract T apply(final U object) throws Exception;
}
