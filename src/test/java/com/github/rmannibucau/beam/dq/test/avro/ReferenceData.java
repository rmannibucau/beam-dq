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
package com.github.rmannibucau.beam.dq.test.avro;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

import java.util.Collection;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class ReferenceData {
    public static final Schema USER_SCHEMA = Schema.createRecord(
            ReferenceData.class.getName() + "User", null, null, false,
            asList(
                    new Schema.Field("name", Schema.create(Schema.Type.STRING), null, (Object) null),
                    new Schema.Field("age", Schema.create(Schema.Type.INT), null, (Object) null),
                    new Schema.Field("income", Schema.create(Schema.Type.DOUBLE), null, (Object) null)));

    public static Collection<GenericRecord> users(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    final GenericData.Record record = new GenericData.Record(USER_SCHEMA);
                    record.put(0, "User_" + i);
                    record.put(1, (30 + i) % 100);
                    record.put(2, (1000 + (i * 100) % 100000) + 0.5);
                    return record;
                })
                .collect(toList());
    }

    public static Create.Values<GenericRecord> sparta() {
        return Create.of(users(100)).withCoder(AvroCoder.of(ReferenceData.USER_SCHEMA));
    }
}
