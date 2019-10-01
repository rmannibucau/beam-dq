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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Locale.ROOT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.rmannibucau.beam.dq.execution.AnalyzeResult;
import com.github.rmannibucau.beam.dq.execution.component.coder.AnalyzeResultCoder;
import com.github.rmannibucau.beam.dq.test.beam.BeamTest;
import com.sun.net.httpserver.HttpServer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;

class HttpPosterTest {
    @BeamTest
    void post(final Pipeline pipeline) throws IOException {
        final AtomicReference<String> received = new AtomicReference<>();
        final HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        final CountDownLatch latch = new CountDownLatch(1);
        server.createContext("/", exchange -> {
            final byte[] bytes = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                received.set("payload=" + reader.lines().collect(joining("\n")) +
                        "\nheaders=" + exchange.getRequestHeaders().entrySet().stream()
                            .filter(it -> Stream.of("date", "host", "user-agent").noneMatch(bl -> it.getKey().toLowerCase(ROOT).contains(bl)))
                            .map(it -> it.getKey() + ": " + String.join(",", it.getValue()))
                            .collect(joining(", ")));
            }
            exchange.close();
            latch.countDown();
        });
        try {
            server.start();
            pipeline.apply(Create.of(new AnalyzeResult(singletonMap("result", 1)))
                    .withCoder(new AnalyzeResultCoder()))
                    .apply(new HttpPoster(
                            new HttpPoster.Configuration(
                                "http://localhost:" + server.getAddress().getPort() + "/post/result",
                                emptyMap()))
                            .toTransform());
            assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
            latch.await(1, MINUTES);
            assertEquals(
                    "payload={\"results\":{\"result\":1}}\n" +
                    "headers=Accept: application/json, Connection: keep-alive, Content-type: application/json, Content-length: 24",
                    received.get());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            server.stop(0);
        }
    }
}
