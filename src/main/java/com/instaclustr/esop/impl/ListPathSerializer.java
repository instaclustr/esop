package com.instaclustr.esop.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ListPathSerializer extends StdSerializer<List<Path>> {

    public ListPathSerializer() {
        super(List.class, false);
    }

    @Override
    public void serialize(final List<Path> value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
        if (value != null) {
            gen.writeStartArray();
            for (final Path p : value) {
                if (p != null) {
                    gen.writeString(p.toString());
                }
            }
            gen.writeEndArray();
        }
    }
}
