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

import static java.util.Collections.singletonList;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import lombok.RequiredArgsConstructor;

public class NormalizeNestedCoGbkResult extends DoFn<CoGbkResult, CoGbkResult> {
    @ProcessElement
    public void onElement(@Element final CoGbkResult result,
                          final OutputReceiver<CoGbkResult> outputReceiver) {
        outputReceiver.output(map(result));
    }

    private CoGbkResult map(final CoGbkResult result) {
        CoGbkResult out = CoGbkResult.empty();
        for (final TupleTag key : result.getSchema().getTupleTagList().getAll()) {
            final Object unwrapped = unwrap(key.getId(), result);
            out = doAdd(out, key, unwrapped);
        }
        return out;
    }

    private Object unwrap(final String key, final CoGbkResult result) {
        final CoGbkResult cbr = result.getOnly(key);
        return cbr.getOnly(key);
    }

    private <A> CoGbkResult doAdd(final CoGbkResult out, final TupleTag<A> key, final A unwrapped) {
        return out.and(key, singletonList(unwrapped));
    }

    public PTransform<PCollection<CoGbkResult>, PCollection<CoGbkResult>> toTransform() {
        return new Tfn(this);
    }

    @RequiredArgsConstructor
    private static class Tfn extends PTransform<PCollection<CoGbkResult>, PCollection<CoGbkResult>> {
        private final NormalizeNestedCoGbkResult fn;

        @Override
        public PCollection<CoGbkResult> expand(final PCollection<CoGbkResult> input) {
            return input.apply("MapCoGbkResultToAnalyzeResult", ParDo.of(fn));
        }
    }
}
