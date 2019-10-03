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
package com.github.rmannibucau.beam.dq.build;

import static java.util.stream.Collectors.joining;
import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.github.rmannibucau.beam.dq.registry.AnalyzerRegistry;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class ReadmeUpdater {
    public static void main(final String[] args) throws IOException {
        final Path path = Paths.get(args[0]); // readme
        final String content = Files.lines(path).collect(joining("\n"));
        final String output = replace(content, "list_of_available_components", generateComponentList());
        if (!output.equals(content)) {
            Files.write(path, output.getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Updated '{}'", path);
        } else {
            log.info("'{}' didn't change", path);
        }
    }

    private static String replace(final String document, final String marker, final String newContent) {
        final String startText = "//begin:" + marker + '\n';
        final int start = document.indexOf(startText);
        final int end = document.indexOf("//end:" + marker + '\n');
        if (start < 0 || end <= start) {
            throw new IllegalStateException("No block found for '" + marker + "'");
        }
        return document.substring(0, start) + startText + '\n' + newContent + '\n' + document.substring(end);
    }

    private static String generateComponentList() {
        return new AnalyzerRegistry().models()
                .map(m -> "=== " + m.getIdentifier() + "\n\n" + m.getDescription() + '\n')
                .sorted()
                .collect(joining("\n"));
    }
}
