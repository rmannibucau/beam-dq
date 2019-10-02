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

import java.util.stream.Stream;

import com.github.rmannibucau.beam.dq.analyzer.base.NumberColumnAnalyser;

public class MinAnalyzer extends NumberColumnAnalyser {
    public MinAnalyzer(final String column) {
        super(column);
    }

    @Override
    protected Double merge(final Stream<Double> stream) {
        return stream.mapToDouble(i -> i)
                .filter(it -> !Double.isNaN(it))
                .min()
                .orElse(Double.NaN);
    }

    @Override
    protected Double accumulate(final Double accumulator, final Number columnValue) {
        return columnValue == null ?
                accumulator :
                Double.isNaN(accumulator) ? columnValue.doubleValue() : Math.min(accumulator, columnValue.doubleValue());
    }
}
