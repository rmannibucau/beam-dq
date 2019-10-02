/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package com.github.rmannibucau.beam.dq.test.beam.BeamAsserts;

import static lombok.AccessLevel.PRIVATE;

import java.io.Serializable;
import java.util.function.Consumer;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public class BeamAssertions {
    public static <T> PTransform<PCollection<T>, PDone> runAssertions(final SerializableConsumer<T> consumer) {
        return new PTransform<PCollection<T>, PDone>() {
            @Override
            public PDone expand(final PCollection<T> input) {
                input.apply("Asserts", ParDo.of(new DoFn<T, Void>() {
                    @ProcessElement
                    public void onElement(@Element final T value) {
                        consumer.accept(value);
                    }
                }));
                return PDone.in(input.getPipeline());
            }
        };
    }

    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
}
