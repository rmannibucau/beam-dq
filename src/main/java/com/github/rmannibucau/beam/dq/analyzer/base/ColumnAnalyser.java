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
package com.github.rmannibucau.beam.dq.analyzer.base;

import static org.apache.beam.sdk.transforms.Combine.globally;

import java.util.LinkedHashMap;
import java.util.function.Function;

import com.github.rmannibucau.beam.dq.cache.SimpleCache;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class ColumnAnalyser<COLUMNTYPE, VALUE, ACCUMULATOR> extends Combine.CombineFn<IndexedRecord, ACCUMULATOR, VALUE> {
    @Getter
    private final String column;

    private transient LinkedHashMap<Schema, Function<IndexedRecord, COLUMNTYPE>> valueExtractor;

    @Override
    public ACCUMULATOR addInput(final ACCUMULATOR mutableAccumulator, final IndexedRecord input) {
        return accumulate(mutableAccumulator, getColumnValue(input));
    }

    protected COLUMNTYPE getColumnValue(final IndexedRecord record) {
        return (valueExtractor == null ? valueExtractor = new SimpleCache<>() : valueExtractor)
                .computeIfAbsent(record.getSchema(), s -> {
                    final int pos = s.getField(column).pos();
                    return r -> mapValue(r.get(pos));
                }).apply(record);
    }

    protected abstract ACCUMULATOR accumulate(ACCUMULATOR accumulator, COLUMNTYPE columnValue);

    protected abstract COLUMNTYPE mapValue(Object columnValue);

    public PTransform<PCollection<? extends IndexedRecord>, PCollection<VALUE>> toTransform() {
        return new AnalyzerTransform(this);
    }

    @RequiredArgsConstructor
    private class AnalyzerTransform extends PTransform<PCollection<? extends IndexedRecord>, PCollection<VALUE>> {
        private final ColumnAnalyser<COLUMNTYPE, VALUE, ACCUMULATOR> analyser;

        @Override
        public PCollection<VALUE> expand(final PCollection<? extends IndexedRecord> input) {
            return input.apply(analyser.getClass().getName() + " on " + analyser.column, globally(analyser));
        }
    }
}
