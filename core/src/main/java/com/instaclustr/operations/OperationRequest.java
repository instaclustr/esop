package com.instaclustr.operations;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.instaclustr.guice.GuiceInjectorHolder;

@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", include = As.EXISTING_PROPERTY)
@JsonTypeIdResolver(OperationRequest.TypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class OperationRequest implements Cloneable {

    @JsonProperty
    public String type;

    static class TypeIdResolver extends MapBackedTypeIdResolver<OperationRequest> {

        public TypeIdResolver() {
            this(GuiceInjectorHolder.INSTANCE.getInjector().getInstance(Key.get(
                new TypeLiteral<Map<String, Class<? extends OperationRequest>>>() {
                }
            )));
        }

        @Inject
        public TypeIdResolver(final Map<String, Class<? extends OperationRequest>> typeMappings) {
            super(typeMappings);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
