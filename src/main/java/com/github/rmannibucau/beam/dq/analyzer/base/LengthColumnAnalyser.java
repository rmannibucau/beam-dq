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

public abstract class LengthColumnAnalyser extends StringColumnAnalyser<Integer, Integer> {
    public LengthColumnAnalyser(final String column) {
        super(column);
    }

    @Override
    protected Integer accumulate(final Integer integer, final String columnValue) {
        return columnValue == null ? integer : combine(integer, columnValue.length());
    }

    @Override
    public Integer extractOutput(final Integer accumulator) {
        return accumulator;
    }

    protected abstract Integer combine(Integer integer, int length);
}
