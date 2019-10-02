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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.github.rmannibucau.beam.dq.analyzer.base.ColumnAnalyser;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

public class MeanAnalyzer extends ColumnAnalyser<Number, Double, MeanAnalyzer.State> {
    public MeanAnalyzer(final String column) {
        super(column);
    }

    @Override
    protected State accumulate(final State state, final Number columnValue) {
        if (columnValue != null) {
            final State out = state == null ? new State() : state;
            out.count++;
            out.sum += columnValue.doubleValue();
            return out;
        }
        return null;
    }

    @Override
    protected Number mapValue(final Object columnValue) {
        return Number.class.isInstance(columnValue) ? Number.class.cast(columnValue) : null;
    }

    @Override
    public State createAccumulator() {
        return new State();
    }

    @Override
    protected State merge(final Stream<State> stream) {
        return stream.collect(Collector.of(State::new, State::add, State::add));
    }

    @Override
    public Double extractOutput(final State accumulator) {
        return accumulator.count == 0 ? Double.NaN : accumulator.sum / accumulator.count;
    }

    @Override
    public Coder<State> getAccumulatorCoder(final CoderRegistry registry, final Coder<IndexedRecord> inputCoder) {
        return new StateCoder();
    }

    public static class State {
        private long count;
        private double sum;

        private State add(final State state2) {
            count += state2.count;
            sum += state2.sum;
            return this;
        }
    }

    private static class StateCoder extends CustomCoder<State> {
        private final VarLongCoder longCoder = VarLongCoder.of();
        private final DoubleCoder doubleCoder = DoubleCoder.of();

        @Override
        public void encode(final State value, final OutputStream outStream) throws IOException {
            longCoder.encode(value.count, outStream);
            doubleCoder.encode(value.sum, outStream);
        }

        @Override
        public State decode(final InputStream inStream) throws IOException {
            final Long count = longCoder.decode(inStream);
            final Double sum = doubleCoder.decode(inStream);
            final State state = new State();
            state.count = count;
            state.sum = sum;
            return state;
        }
    }
}
