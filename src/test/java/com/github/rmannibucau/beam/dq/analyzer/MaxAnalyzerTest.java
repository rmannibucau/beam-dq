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
package com.github.rmannibucau.beam.dq.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Serializable;

import com.github.rmannibucau.beam.dq.test.avro.ReferenceData;
import com.github.rmannibucau.beam.dq.test.beam.BeamTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

class MaxAnalyzerTest implements Serializable {
    @BeamTest
    void computeMax(final Pipeline pipeline) {
        pipeline.apply("Seed", ReferenceData.sparta())
                .apply("Max", new MaxAnalyzer("income").toTransform())
                .apply("Asserts", ParDo.of(new DoFn<Double, Void>() {
                    @ProcessElement
                    public void onElement(@Element final Double value) {
                        assertEquals(10900.5, value);
                    }
                }));
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }
}
