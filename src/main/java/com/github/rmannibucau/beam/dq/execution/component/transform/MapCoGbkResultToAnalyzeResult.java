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
package com.github.rmannibucau.beam.dq.execution.component.transform;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.github.rmannibucau.beam.dq.execution.AnalyzeResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import lombok.RequiredArgsConstructor;

public class MapCoGbkResultToAnalyzeResult extends DoFn<CoGbkResult, AnalyzeResult> {
    @ProcessElement
    public void onElement(@Element final CoGbkResult result,
                          final OutputReceiver<AnalyzeResult> outputReceiver) {
        outputReceiver.output(map(result));
    }

    private AnalyzeResult map(final CoGbkResult result) {
        return new AnalyzeResult(result.getSchema().getTupleTagList().getAll().stream()
            .map(TupleTag::getId)
            .collect(toMap(identity(), key -> extractValue(result, key))));
    }

    private Object extractValue(final CoGbkResult result, final String t) {
        return result.getOnly(t);
    }

    public PTransform<PCollection<CoGbkResult>, PCollection<AnalyzeResult>> toTransform() {
        return new Tfn(this);
    }

    @RequiredArgsConstructor
    private static class Tfn extends PTransform<PCollection<CoGbkResult>, PCollection<AnalyzeResult>> {
        private final MapCoGbkResultToAnalyzeResult fn;

        @Override
        public PCollection<AnalyzeResult> expand(final PCollection<CoGbkResult> input) {
            return input.apply("MapCoGbkResultToAnalyzeResult", ParDo.of(fn));
        }
    }
}
