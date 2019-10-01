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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NormalizeOutput<A> extends DoFn<A, KV<Integer, CoGbkResult>> {
    private final String name;

    @ProcessElement
    public void wrap(@Element final A input, final OutputReceiver<KV<Integer, CoGbkResult>> output) {
        output.output(KV.of(1, CoGbkResult.of(new TupleTag<>(name), singletonList(input))));
    }

    public PTransform<PCollection<A>, PCollection<KV<Integer, CoGbkResult>>> toTransform() {
        return new Tfn<>(this);
    }

    @RequiredArgsConstructor
    private static class Tfn<A> extends PTransform<PCollection<A>, PCollection<KV<Integer, CoGbkResult>>> {
        private final NormalizeOutput<A> fn;

        @Override
        public PCollection<KV<Integer, CoGbkResult>> expand(final PCollection<A> input) {
            return input.apply("EnforceConstantKey", ParDo.of(fn));
        }
    }
}
