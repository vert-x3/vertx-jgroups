/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.java.spi.cluster.impl.jgroups.support;

import io.vertx.core.logging.Logger;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface LambdaLogger {

  Logger log();

  default <T> void logError(Supplier<Stream<T>> supplier, Function<T, String> transformer) {
    supplier.get()
        .map(transformer)
        .forEach(log()::error);
  }

  default void logError(Supplier<String> message) {
    log().error(message.get());
  }

  default <T> void logTrace(Supplier<Stream<T>> supplier, Function<T, String> transformer) {
    if (log().isTraceEnabled()) {
      supplier.get()
          .map(transformer).forEach(log()::trace);
    }
  }

  default void logTrace(Supplier<String> message) {
    if (log().isTraceEnabled()) {
      log().trace(message.get());
    }
  }

  default <T> void logDebug(Supplier<List<T>> supplier, Function<T, String> transformer) {
    if (log().isTraceEnabled()) {
      supplier.get().forEach((t) -> log().debug(transformer.apply(t)));
    }
  }

  default void logDebug(Supplier<String> message) {
    if (log().isDebugEnabled()) {
      log().debug(message.get());
    }
  }

  default <T> void logInfo(Supplier<List<T>> supplier, Function<T, String> transformer) {
    if (log().isTraceEnabled()) {
      supplier.get().forEach((t) -> log().info(transformer.apply(t)));
    }
  }

  default void logInfo(Supplier<String> message) {
    if (log().isInfoEnabled()) {
      log().info(message.get());
    }
  }
}
