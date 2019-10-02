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
package com.github.rmannibucau.beam.dq.config;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import com.github.rmannibucau.beam.dq.execution.AnalyzeRequest;
import org.junit.jupiter.api.Test;

class JsonConfigTest {
    @Test
    void deserialize() throws Exception {
        try (final Jsonb jsonb = JsonbBuilder.create();
             final BufferedReader reader = new BufferedReader(new InputStreamReader(
                     ofNullable(Thread.currentThread().getContextClassLoader())
                             .orElseGet(ClassLoader::getSystemClassLoader)
                             .getResourceAsStream("analyze-request.json"),
                     StandardCharsets.UTF_8))) {
            final String json = reader.lines().collect(joining("\n"));
            final AnalyzeRequest request = jsonb.fromJson(json, AnalyzeRequest.class);
            assertEquals(5, request.getAnalyzers().size());
            assertEquals(
                    "max_income => com.github.rmannibucau.beam.dq.analyzer.MaxAnalyzer(income)\n" +
                    "yougest => com.github.rmannibucau.beam.dq.analyzer.MinAnalyzer(age)\n" +
                    "medium => com.github.rmannibucau.beam.dq.analyzer.MeanAnalyzer(age)\n" +
                    "shortest_name => com.github.rmannibucau.beam.dq.analyzer.MinLengthAnalyzer(name)\n" +
                    "longest_name => com.github.rmannibucau.beam.dq.analyzer.MaxLengthAnalyzer(name)",
                    request.getAnalyzers().stream()
                        .map(it -> it.getName() + " => " + it.getAnalyzer().getClass().getName() + '(' + it.getAnalyzer().getColumn() + ')')
                        .collect(joining("\n")));
        }
    }
}
