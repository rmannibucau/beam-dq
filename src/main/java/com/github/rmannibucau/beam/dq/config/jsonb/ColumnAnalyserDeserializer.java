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

import java.io.StringReader;
import java.lang.reflect.Type;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.bind.serializer.DeserializationContext;
import javax.json.bind.serializer.JsonbDeserializer;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

import com.github.rmannibucau.beam.dq.analyzer.base.ColumnAnalyser;
import com.github.rmannibucau.beam.dq.registry.AnalyzerRegistry;

public class ColumnAnalyserDeserializer<A, B, C> implements JsonbDeserializer<ColumnAnalyser<A, B, C>> {
    private final JsonParserFactory factory = Json.createParserFactory(emptyMap());
    private final AnalyzerRegistry registry = new AnalyzerRegistry();

    @Override
    public ColumnAnalyser<A, B, C> deserialize(final JsonParser parser, final DeserializationContext ctx, final Type type) {
        final JsonObject object = ctx.deserialize(JsonObject.class, parser);
        final Class<? extends ColumnAnalyser> analyzerType = registry.findById(object.getString("@type"))
                .map(AnalyzerRegistry.AnalyzerRegistrationModel::getAnalyzerType)
                .orElseThrow(() -> new IllegalArgumentException("Unknown type in: '" + object + "'"));
        try (final JsonParser jsonParser = factory.createParser(new StringReader(object.toString()))){
            return  ctx.deserialize(analyzerType, jsonParser);
        }
    }
}
