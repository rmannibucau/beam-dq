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
package com.github.rmannibucau.beam.dq.test.beam;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Optional.ofNullable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

@Test
@Target(METHOD)
@Retention(RUNTIME)
@ExtendWith(BeamTest.Impl.class)
public @interface BeamTest {
    String[] options() default {};

    class Impl implements BeforeEachCallback, AfterEachCallback, ParameterResolver {
        private final ExtensionContext.Namespace NS = ExtensionContext.Namespace.create(getClass().getEnclosingClass());

        @Override
        public void beforeEach(final ExtensionContext context) {
            getConfig(context).ifPresent(config -> {
                final ExtensionContext.Store store = context.getStore(NS);
                final PipelineOptions options = PipelineOptionsFactory.fromArgs(config.options()).create();
                store.put(PipelineOptions.class, options);
                store.put(Pipeline.class, new Pipeline(options) {
                    @Override
                    public PipelineResult run(final PipelineOptions options) {
                        final PipelineResult run = super.run(options);
                        store.put(PipelineResult.class, run);
                        return run;
                    }
                });
            });
        }

        @Override
        public void afterEach(final ExtensionContext context) {
            // ensure we waited to full execution and dont leak a run outside a test scope
            getConfig(context)
                    .flatMap(config -> ofNullable(context.getStore(NS).get(PipelineResult.class, PipelineResult.class)))
                    .filter(p -> {
                        switch (p.getState()) {
                            case DONE:
                            case FAILED:
                            case STOPPED:
                            case CANCELLED:
                                return false;
                            case RUNNING:
                            case UNKNOWN:
                            case UPDATED:
                            default:
                                return true;
                        }
                    })
                    .ifPresent(PipelineResult::waitUntilFinish);
        }

        @Override
        public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
            return getConfig(extensionContext).isPresent() && extensionContext.getStore(NS).get(parameterContext.getParameter().getType()) != null;
        }

        @Override
        public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
            return extensionContext.getStore(NS).get(parameterContext.getParameter().getType());
        }

        private Optional<BeamTest> getConfig(final ExtensionContext context) {
            return context.getElement().map(e -> e.getAnnotation(BeamTest.class));
        }
    }
}
