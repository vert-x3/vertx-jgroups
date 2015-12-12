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

package io.vertx.spi.cluster.jgroups.services;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.spi.cluster.jgroups.support.DataHolder;
import io.vertx.spi.cluster.jgroups.support.LambdaLogger;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultRpcMapService implements RpcMapService, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(DefaultRpcMapService.class);

  private final Map<String, Map> maps = new ConcurrentHashMap<>();

  @Override
  public <K, V> Map<K, V> mapCreate(String name) {
    logTrace(() -> String.format("method mapCreate name[%s]", name));
    return maps.computeIfAbsent(name, (key) -> new ConcurrentHashMap());
  }

  @Override
  public <K, V> void mapPut(String name, DataHolder<K> k, DataHolder<V> v) {
    logTrace(() -> "RpcMapService.put name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    this.<K, V>execute(name, (map) -> map.put(k.unwrap(), v.unwrap()));
  }

  @Override
  public <K, V> DataHolder<V> mapPutIfAbsent(String name, DataHolder<K> k, DataHolder<V> v) {
    logTrace(() -> "RpcMapService.putIfAbsent name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.putIfAbsent(k.unwrap(), v.unwrap())));
  }

  @Override
  public <K, V> DataHolder<V> mapRemove(String name, DataHolder<K> k) {
    logTrace(() -> "RpcMapService.remove name = [" + name + "], k = [" + k + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.remove(k.unwrap())));
  }

  @Override
  public <K, V> boolean mapRemoveIfPresent(String name, DataHolder<K> k, DataHolder<V> v) {
    logTrace(() -> "RpcMapService.removeIfPresent name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, Boolean>executeAndReturn(name, (map) -> map.remove(k.unwrap(), v.unwrap()));
  }

  @Override
  public <K, V> DataHolder<V> mapReplace(String name, DataHolder<K> k, DataHolder<V> v) {
    logTrace(() -> "RpcMapService.replace name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.replace(k.unwrap(), v.unwrap())));
  }

  @Override
  public <K, V> boolean mapReplaceIfPresent(String name, DataHolder<K> k, DataHolder<V> oldValue, DataHolder<V> newValue) {
    logTrace(() -> "RpcMapService.removeIfPresent name = [" + name + "], k = [" + k + "], oldValue = [" + oldValue + "], newValue = [" + newValue + "]");
    return this.<K, V, Boolean>executeAndReturn(name, (map) -> map.replace(k.unwrap(), oldValue.unwrap(), newValue.unwrap()));
  }

  @Override
  public <K, V> void mapClear(String name) {
    logTrace(() -> "RpcMapService.clear name = [" + name + "]");
    this.<K, V>execute(name, Map::clear);
  }

  @Override
  public <K, V> void mapPutAll(String name, Map<DataHolder<K>, DataHolder<V>> m) {
    logTrace(() -> "RpcMapService.mapPutAll name = [" + name + "]");
    this.execute(name, (map) -> m.forEach((k, v) -> map.put(k.unwrap(), v.unwrap())));
  }

  @Override
  public void writeTo(OutputStream output) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(output)) {
      oos.writeObject(maps);
      oos.flush();
    }
  }

  @Override
  public void readFrom(InputStream input) throws IOException, ClassNotFoundException {
    try (ObjectInputStream oos = new ObjectInputStream(input)) {
      Map<String, Map> m = (Map<String, Map>) oos.readObject();
      maps.putAll(m);
    }
  }

  private <K, V, R> R executeAndReturn(String name, Function<Map<K, V>, R> function) {
    return function.apply((Map<K, V>) maps.computeIfAbsent(name, (k) -> new ConcurrentHashMap()));
  }

  private <K, V> void execute(String name, Consumer<Map<K, V>> consumer) {
    this.<K, V, Void>executeAndReturn(name, (map) -> {
      consumer.accept(map);
      return null;
    });
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
