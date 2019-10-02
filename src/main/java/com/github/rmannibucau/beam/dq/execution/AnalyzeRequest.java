/**
 *
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
package com.github.rmannibucau.beam.dq.execution;

import java.util.Collection;

import javax.json.bind.annotation.JsonbTypeDeserializer;

import com.github.rmannibucau.beam.dq.analyzer.base.ColumnAnalyser;
import com.github.rmannibucau.beam.dq.config.jsonb.ColumnAnalyserDeserializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnalyzeRequest {
    private Collection<AnalyzerRequest<?>> analyzers;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnalyzerRequest<O> {
        private String name;

        @JsonbTypeDeserializer(ColumnAnalyserDeserializer.class)
        private ColumnAnalyser<?, O, ?> analyzer;
    }
}
