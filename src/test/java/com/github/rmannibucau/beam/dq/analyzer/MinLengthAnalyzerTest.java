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

import static com.github.rmannibucau.beam.dq.test.beam.BeamAsserts.BeamAssertions.runAssertions;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Serializable;

import com.github.rmannibucau.beam.dq.test.avro.ReferenceData;
import com.github.rmannibucau.beam.dq.test.beam.BeamTest;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

class MinLengthAnalyzerTest implements Serializable {
    @BeamTest
    void computeMin(final Pipeline pipeline) {
        assertEquals(
            PipelineResult.State.DONE,
            pipeline.apply("Seed", ReferenceData.sparta())
                    .apply("MinLength", new MinLengthAnalyzer("name").toTransform())
                    .apply("Asserts", runAssertions(value -> assertEquals(6, value)))
                    .getPipeline()
                    .run()
                    .waitUntilFinish());
    }
}
