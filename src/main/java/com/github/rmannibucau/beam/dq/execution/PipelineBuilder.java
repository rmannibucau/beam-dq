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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.transforms.Combine.globally;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import com.github.rmannibucau.beam.dq.execution.component.coder.AnalyzeResultCoder;
import com.github.rmannibucau.beam.dq.execution.component.combine.EmitOneRecord;
import com.github.rmannibucau.beam.dq.execution.component.transform.EmptyKeyedCoGroupResult;
import com.github.rmannibucau.beam.dq.execution.component.transform.MapCoGbkResultToAnalyzeResult;
import com.github.rmannibucau.beam.dq.execution.component.transform.NormalizeNestedCoGbkResult;
import com.github.rmannibucau.beam.dq.execution.component.transform.NormalizeOutput;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class PipelineBuilder {
    public PCollection<AnalyzeResult> build(final PCollection<? extends IndexedRecord> input, final AnalyzeRequest request) {
        final SortedMap<String, PCollection<KV<Integer, CoGbkResult>>> outputs = new TreeMap<>(ofNullable(request.getAnalyzers())
                .orElseGet(Collections::emptyList).stream()
                .map(this::normalize)
                .collect(toMap(AnalyzeRequest.AnalyzerRequest::getName, req -> createKeyedPartialResult(input, req))));
        switch (outputs.size()) {
            case 0: // should likely be handled way early in the processing to bypass the pipeline but just a sanity case
                return mapResult(input
                        .apply(globally(new EmitOneRecord<>())) // skip records, we don't compute anything
                        .setCoder(VarIntCoder.of())
                        .apply(new EmptyKeyedCoGroupResult<Integer>().toTransform())
                        .setCoder(KvCoder.of(VarIntCoder.of(), CoGbkResult.CoGbkResultCoder.of(
                                CoGbkResultSchema.of(emptyList()), UnionCoder.of(emptyList())))), identity());
            case 1: // bypass joins and potential shuffles, just process the only output
                return mapResult(outputs.entrySet().iterator().next().getValue(), identity());
            default: // now join all outputs to get a single result - normally shuffle is 1 record so "ok"
                KeyedPCollectionTuple<Integer> globalOutput = KeyedPCollectionTuple.empty(input.getPipeline());
                for (final Map.Entry<String, PCollection<KV<Integer, CoGbkResult>>> entry : outputs.entrySet()) {
                    globalOutput = globalOutput.and(entry.getKey(), entry.getValue());
                }
                final PCollection<KV<Integer, CoGbkResult>> global = globalOutput.apply("CoGroupByKeyResult", CoGroupByKey.create());
                return mapResult(global, cbr -> {
                    final CoGbkResult.CoGbkResultCoder wrappedCoder = CoGbkResult.CoGbkResultCoder.class.cast(cbr.getCoder());
                    final CoGbkResult.CoGbkResultCoder unwrappedCoder = CoGbkResult.CoGbkResultCoder.of(
                            wrappedCoder.getSchema(),
                            UnionCoder.of(wrappedCoder.getUnionCoder().getElementCoders().stream()
                                    .flatMap(it -> CoGbkResult.CoGbkResultCoder.class.isInstance(it) ?
                                            CoGbkResult.CoGbkResultCoder.class.cast(it).getUnionCoder().getComponents().stream() :
                                            Stream.of(it))
                                    .collect(toList())));
                    return cbr.apply("NormalizeNestedCoGbkResult", new NormalizeNestedCoGbkResult().toTransform())
                            .setCoder(unwrappedCoder);
                });
        }
    }

    private PCollection<AnalyzeResult> mapResult(final PCollection<KV<Integer, CoGbkResult>> coGroupByKeyResult,
                                                 final Function<PCollection<CoGbkResult>, PCollection<CoGbkResult>> preprocessor) {
        final PCollection<CoGbkResult> unwrappedResult = coGroupByKeyResult.apply("UnwrapResult", Values.create());
        return preprocessor.apply(unwrappedResult)
                .apply("MapResult", new MapCoGbkResultToAnalyzeResult().toTransform())
                .setCoder(new AnalyzeResultCoder());
    }

    private <A> PCollection<KV<Integer, CoGbkResult>> createKeyedPartialResult(final PCollection<? extends IndexedRecord> input,
                                                                               final AnalyzeRequest.AnalyzerRequest<A> req) {
        final PCollection<A> applied = input.apply(req.getName(), req.getAnalyzer().toTransform());
        return applied
                .apply("EnforceConstantKey for " + req.getName(), new NormalizeOutput<A>(req.getName()).toTransform())
                .setCoder(KvCoder.of(VarIntCoder.of(), CoGbkResult.CoGbkResultCoder.of(
                        CoGbkResultSchema.of(singletonList(new TupleTag<>(req.getName()))),
                        UnionCoder.of(singletonList(applied.getCoder())))));
    }

    private <A> AnalyzeRequest.AnalyzerRequest<A> normalize(final AnalyzeRequest.AnalyzerRequest<A> req) {
        requireNonNull(req.getAnalyzer(), "No analyzer set for " + req);
        return req.getName() == null ? new AnalyzeRequest.AnalyzerRequest<>(randomName(req), req.getAnalyzer()) : req;
    }

    private <A> String randomName(AnalyzeRequest.AnalyzerRequest<A> req) {
        return req.getAnalyzer().getClass().getName() + " on " +
                req.getAnalyzer().getColumn() + " (" +
                UUID.randomUUID().toString() + ')';
    }
}
