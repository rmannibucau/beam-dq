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

import static java.util.Optional.ofNullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import com.github.rmannibucau.beam.dq.execution.AnalyzeResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class HttpPoster extends DoFn<AnalyzeResult, Void> {
    private final Configuration configuration;

    @ProcessElement
    public void onElement(@Element final AnalyzeResult result) {
        try {
            final URL url = new URL(configuration.getUrl());
            final HttpURLConnection httpURLConnection = HttpURLConnection.class.cast(url.openConnection());
            httpURLConnection.setDoOutput(true);
            ofNullable(configuration.getHeaders()).ifPresent(headers -> headers.forEach(httpURLConnection::setRequestProperty));
            if (configuration.getHeaders() == null || !configuration.getHeaders().containsKey("Content-Type")) {
                httpURLConnection.setRequestProperty("Content-Type", "application/json");
            }
            if (configuration.getHeaders() == null || !configuration.getHeaders().containsKey("Accept")) {
                httpURLConnection.setRequestProperty("Accept", "application/json");
            }

            try (final Jsonb jsonb = JsonbBuilder.create();
                 final OutputStream out = httpURLConnection.getOutputStream()) {
                jsonb.toJson(result, out);

                String stdout;
                try (final InputStream inputStream = httpURLConnection.getInputStream()) {
                    stdout = slurp(inputStream);
                } catch (final IOException ioe) {
                    stdout = "(error) " + ioe.getMessage();
                }

                String stderr;
                try (final InputStream inputStream = httpURLConnection.getErrorStream()) {
                    stderr = slurp(inputStream);
                } catch (final IOException ioe) {
                    stderr = "(error) " + ioe.getMessage();
                }

                log.info("Response: status=" + httpURLConnection.getResponseCode() + ", message=" + stdout + ", error=" + stderr);
            } catch (final IOException e) {
                throw e;
            } catch (final Exception e) {
                throw new IOException(e);
            } finally {
                httpURLConnection.disconnect();
            }
        } catch (final MalformedURLException e) {
            throw new IllegalArgumentException(e);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String slurp(final InputStream stream) throws IOException {
        if (stream == null) {
            return "<empty>";
        }

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final byte[] buffer = new byte[512];
        int read;
        while ((read = stream.read(buffer)) >= 0) {
            if (read > 0) {
                output.write(buffer, 0, read);
            }
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    public PTransform<PCollection<AnalyzeResult>, PDone> toTransform() {
        return new Tfn(this);
    }

    @RequiredArgsConstructor
    private static class Tfn extends PTransform<PCollection<AnalyzeResult>, PDone> {
        private final HttpPoster fn;

        @Override
        public PDone expand(final PCollection<AnalyzeResult> input) {
            input.apply("HttpPoster @ " + fn.configuration.url, ParDo.of(fn));
            return PDone.in(input.getPipeline());
        }
    }

    @Data
    @Builder
    public static class Configuration implements Serializable {
        private String url;
        private Map<String, String> headers;
    }
}
