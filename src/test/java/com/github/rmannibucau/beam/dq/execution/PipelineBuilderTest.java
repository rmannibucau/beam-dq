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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import com.github.rmannibucau.beam.dq.analyzer.MaxAnalyzer;
import com.github.rmannibucau.beam.dq.test.avro.ReferenceData;
import com.github.rmannibucau.beam.dq.test.beam.BeamTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

class PipelineBuilderTest implements Serializable {
    @BeamTest
    void computeTwoResults(final Pipeline pipeline) {
        new PipelineBuilder()
                .build(
                    pipeline.apply("Seed", ReferenceData.sparta()),
                    new AnalyzeRequest(asList(
                        new AnalyzeRequest.AnalyzerRequest<>("richest", new MaxAnalyzer("income")),
                        new AnalyzeRequest.AnalyzerRequest<>("oldest", new MaxAnalyzer("age")))))
                .apply("Asserts", ParDo.of(new DoFn<AnalyzeResult, Void>() {
                    @ProcessElement
                    public void onElement(@Element final AnalyzeResult value) {
                        final Map<String, Object> results = value.getResults();
                        assertEquals(2, results.size(), value::toString);
                        assertEquals(BigDecimal.valueOf(10900.5).doubleValue(), Number.class.cast(results.get("richest")).doubleValue(), results::toString);
                        assertEquals(BigDecimal.valueOf(99.).doubleValue(), Number.class.cast(results.get("oldest")).doubleValue(), results::toString);
                    }
                }));
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @BeamTest
    void computeNoResults(final Pipeline pipeline) {
        new PipelineBuilder()
                .build(
                    pipeline.apply("Seed", ReferenceData.sparta()),
                    new AnalyzeRequest(emptyList()))
                .apply("Asserts", ParDo.of(new DoFn<AnalyzeResult, Void>() {
                    @ProcessElement
                    public void onElement(@Element final AnalyzeResult value) {
                        assertEquals(0, value.getResults().size(), value::toString);
                    }
                }));
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @BeamTest
    void computeOneResults(final Pipeline pipeline) {
        new PipelineBuilder()
                .build(
                    pipeline.apply("Seed", ReferenceData.sparta()),
                    new AnalyzeRequest(singletonList(
                            new AnalyzeRequest.AnalyzerRequest<>("oldest", new MaxAnalyzer("age")))))
                .apply("Asserts", ParDo.of(new DoFn<AnalyzeResult, Void>() {
                    @ProcessElement
                    public void onElement(@Element final AnalyzeResult value) {
                        final Map<String, Object> results = value.getResults();
                        assertEquals(1, results.size(), value::toString);
                        assertEquals(BigDecimal.valueOf(99.).doubleValue(), Number.class.cast(results.get("oldest")).doubleValue(), results::toString);
                    }
                }));
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }
}
