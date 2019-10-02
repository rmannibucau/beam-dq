/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.rmannibucau.beam.dq.config.jsonb;

import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.stream.Stream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.bind.serializer.DeserializationContext;
import javax.json.bind.serializer.JsonbDeserializer;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

import com.github.rmannibucau.beam.dq.analyzer.base.ColumnAnalyser;

public class ColumnAnalyserDeserializer<A, B, C> implements JsonbDeserializer<ColumnAnalyser<A, B, C>> {
    private final JsonParserFactory factory = Json.createParserFactory(emptyMap());

    @Override
    public ColumnAnalyser<A, B, C> deserialize(final JsonParser parser, final DeserializationContext ctx, final Type type) {
        final JsonObject object = ctx.deserialize(JsonObject.class, parser);
        final Class<?> analyzerType = getPotentialPackages()
                .map(p -> p + '.' + object.getString("@type"))
                .map(this::load)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown type in: '" + object + "'"));
        try (final JsonParser jsonParser = factory.createParser(new StringReader(object.toString()))){
            return (ColumnAnalyser<A, B, C>) ctx.deserialize(analyzerType, jsonParser);
        }
    }

    private Class<?> load(final String fqn) {
        try {
            return ofNullable(Thread.currentThread().getContextClassLoader()).orElseGet(ClassLoader::getSystemClassLoader)
                    .loadClass(fqn);
        } catch (final ClassNotFoundException e) {
            return null;
        }
    }

    private Stream<String> getPotentialPackages() {
        return Stream.of("com.github.rmannibucau.beam.dq.analyzer");
    }
}
