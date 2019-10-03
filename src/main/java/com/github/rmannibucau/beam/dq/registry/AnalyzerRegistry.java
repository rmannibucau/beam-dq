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
package com.github.rmannibucau.beam.dq.registry;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

import com.github.rmannibucau.beam.dq.analyzer.api.AnalyzerDefinition;
import com.github.rmannibucau.beam.dq.analyzer.api.Dimension;
import com.github.rmannibucau.beam.dq.analyzer.base.ColumnAnalyser;

import lombok.Data;

public class AnalyzerRegistry {
    private final Map<String, AnalyzerRegistrationModel<?, ?, ?>> analysers = new HashMap<>();

    public AnalyzerRegistry() {
        final ClassLoader loader = ofNullable(Thread.currentThread().getContextClassLoader())
                .orElseGet(ClassLoader::getSystemClassLoader);
        try {
            final Enumeration<URL> spi = loader.getResources("META-INF/services/" + ColumnAnalyser.class.getName());
            while (spi.hasMoreElements()) {
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(spi.nextElement().openStream(), StandardCharsets.UTF_8))) {

                    analysers.putAll(reader.lines()
                            .map(String::trim)
                            .filter(it -> !it.startsWith("#") && !it.isEmpty())
                            .map(it -> {
                                try {
                                    return (Class<ColumnAnalyser>) loader.loadClass(it);
                                } catch (final ClassNotFoundException e) {
                                    throw new IllegalStateException(e);
                                }
                            })
                            .map(this::toRegistrationModel)
                            .collect(toMap(AnalyzerRegistrationModel::getIdentifier, identity())));
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public <A, B, C> Optional<AnalyzerRegistrationModel<A, B, C>> findById(final String id) {
        return ofNullable(analysers.get(id)).map(it -> (AnalyzerRegistrationModel<A, B, C>) it);
    }

    public Stream<AnalyzerRegistrationModel<?, ?, ?>> models() {
        return analysers.values().stream();
    }

    private <A, B, C> AnalyzerRegistrationModel<A, B, C> toRegistrationModel(final Class<ColumnAnalyser> type) {
        final AnalyzerDefinition def = requireNonNull(
                type.getAnnotation(AnalyzerDefinition.class),
"Missing @AnalyzerDefinition on " + type.getName());
        return new AnalyzerRegistrationModel(
                Optional.of(def.identifier()).filter(it -> !it.isEmpty()).orElseGet(type::getSimpleName),
                new TreeSet<>(Stream.of(def.dimensions()).collect(toSet())),
                def.description(),
                type);
    }

    @Data
    public static class AnalyzerRegistrationModel<A, B, C> {
        private final String identifier;
        private final Collection<Dimension> dimensions;
        private final String description;
        private final Class<ColumnAnalyser<A, B, C>> analyzerType;
    }
}
