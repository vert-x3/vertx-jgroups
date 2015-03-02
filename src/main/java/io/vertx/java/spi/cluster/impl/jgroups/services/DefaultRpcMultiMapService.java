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

package io.vertx.java.spi.cluster.impl.jgroups.services;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.java.spi.cluster.impl.jgroups.domain.MultiMapImpl;
import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class DefaultRpcMultiMapService implements RpcMultiMapService, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(DefaultRpcMultiMapService.class);

  private final Map<String, MultiMapImpl> maps;

  public DefaultRpcMultiMapService(Map<String, MultiMapImpl> maps) {
    this.maps = maps;
  }

  public <K, V, R> R executeAndReturn(String name, Function<MultiMapImpl<K, V>, R> function) {
    MultiMapImpl map = Optional
        .ofNullable(maps.get(name))
        .orElseThrow(() -> new IllegalStateException(String.format("MultiMapImpl [%s] not found.", name)));
    return function.apply((MultiMapImpl<K, V>) map);
  }

  public <K, V> MultiMapImpl<K, V> multiMapGet(String name) {
    logDebug(() -> String.format("method multiMapGet name[%s]", name));
    return this.<K, V, MultiMapImpl<K, V>>executeAndReturn(name, Function.identity());
  }

  public boolean multiMapCreate(String name) {
    logDebug(() -> String.format("method multiMapCreate name[%s]", name));
    maps.computeIfAbsent(name, (key) -> new MultiMapImpl());
    return true;
  }

  public <K, V> void multiMapAdd(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> String.format("method multiMapAdd name[%s] key[%s] value[%s]", name, k, v));
    this.<K, V, Void>executeAndReturn(name, (map) -> {
      map.add(k.unwrap(), v.unwrap());
      return null;
    });
  }

  public <K, V> boolean multiMapRemove(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> String.format("method multiMapRemove name[%s] key[%s] value[%s]", name, k, v));
    return this.<K, V, Boolean>executeAndReturn(name, (map) -> map.remove(k.unwrap(), v.unwrap()));
  }

  public <K, V> void multiMapRemoveAll(String name, DataHolder<V> v) {
    logDebug(() -> String.format("method multiMapRemoveAll name[%s] value[%s]", name, v));
    this.<K, V, Void>executeAndReturn(name, (map) -> {
      map.removeAll(v.unwrap());
      return null;
    });
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
