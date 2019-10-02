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

import com.github.rmannibucau.beam.dq.analyzer.base.LengthColumnAnalyser;

public class MinLengthAnalyzer extends LengthColumnAnalyser {
    public MinLengthAnalyzer(final String column) {
        super(column);
    }

    @Override
    protected Integer combine(final Integer integer, final int length) {
        return Math.min(integer, length);
    }

    @Override
    public Integer createAccumulator() {
        return Integer.MAX_VALUE;
    }

    @Override
    protected Integer merge(final Stream<Integer> stream) {
        return stream.mapToInt(i -> i).min().orElseGet(this::createAccumulator);
    }
}
