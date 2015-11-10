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
import io.vertx.core.logging.LoggerFactory;
import io.vertx.java.spi.cluster.impl.jgroups.domain.MultiMap;
import io.vertx.java.spi.cluster.impl.jgroups.domain.MultiMapImpl;
import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DefaultRpcMultiMapService implements RpcMultiMapService, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(DefaultRpcMultiMapService.class);

  private final Map<String, MultiMap> maps = new ConcurrentHashMap<>();

  public <K, V> MultiMap<K, V> multiMapCreate(String name) {
    logDebug(() -> String.format("method multiMapCreate key[%s]", name));
    return this.<K, V, MultiMap<K, V>>executeAndReturn(name, Function.identity());
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
  public void writeTo(OutputStream output) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(output)) {
      oos.writeObject(maps);
    }
  }

  @Override
  public void readFrom(InputStream input) throws IOException, ClassNotFoundException {
    try (ObjectInputStream oos = new ObjectInputStream(input)) {
      Map<String, MultiMap> m = (Map<String, MultiMap>) oos.readObject();
      maps.putAll(m);
    }
  }

  private <K, V, R> R executeAndReturn(String name, Function<MultiMap<K, V>, R> function) {
    MultiMap<K, V> map = maps.computeIfAbsent(name, (key) -> {
      logDebug(() -> String.format("create multiMap with name[%s]", key));
      return new MultiMapImpl<K, V>(key);
    });
    return function.apply(map);
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
