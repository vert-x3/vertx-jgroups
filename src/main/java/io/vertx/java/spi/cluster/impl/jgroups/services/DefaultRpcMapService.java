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
import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultRpcMapService implements RpcMapService, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(DefaultRpcMapService.class);

  private final Map<String, Map> maps;

  public DefaultRpcMapService(Map<String, Map> maps) {
    this.maps = maps;
  }

  private <K, V> void execute(String name, Consumer<Map<K, V>> consumer) {
    this.<K, V, Void>executeAndReturn(name, (map) -> {
      consumer.accept(map);
      return null;
    });
  }

  public <K, V, R> R executeAndReturn(String name, Function<Map<K, V>, R> function) {
    return function.apply((Map<K, V>) maps.computeIfAbsent(name, (k) -> new ConcurrentHashMap()));
  }

  public <K, V> Map<K, V> mapGet(String name) {
    return this.<K, V, Map<K, V>>executeAndReturn(name, Function.identity());
  }

  @Override
  public <K, V> boolean mapCreate(String name) {
    logDebug(() -> String.format("method mapCreate name[%s]", name));
    maps.computeIfAbsent(name, (key) -> new ConcurrentHashMap());
    return true;
  }

  @Override
  public <K, V> void mapPut(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> "RpcMapService.put name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    this.<K, V>execute(name, (map) -> map.put(k.unwrap(), v.unwrap()));
  }

  @Override
  public <K, V> DataHolder<V> mapPutIfAbsent(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> "RpcMapService.putIfAbsent name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.putIfAbsent(k.unwrap(), v.unwrap())));
  }

  @Override
  public <K, V> DataHolder<V> mapRemove(String name, DataHolder<K> k) {
    logDebug(() -> "RpcMapService.remove name = [" + name + "], k = [" + k + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.remove(k.unwrap())));
  }

  @Override
  public <K, V> boolean mapRemoveIfPresent(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> "RpcMapService.removeIfPresent name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, Boolean>executeAndReturn(name, (map) -> map.remove(k.unwrap(), v.unwrap()));
  }

  @Override
  public <K, V> DataHolder<V> mapReplace(String name, DataHolder<K> k, DataHolder<V> v) {
    logDebug(() -> "RpcMapService.replace name = [" + name + "], k = [" + k + "], v = [" + v + "]");
    return this.<K, V, DataHolder<V>>executeAndReturn(name, (map) -> DataHolder.wrap(map.replace(k.unwrap(), v.unwrap())));
  }

  @Override
  public <K, V> boolean mapReplaceIfPresent(String name, DataHolder<K> k, DataHolder<V> oldValue, DataHolder<V> newValue) {
    logDebug(() -> "RpcMapService.removeIfPresent name = [" + name + "], k = [" + k + "], oldValue = [" + oldValue + "], newValue = [" + newValue + "]");
    return this.<K, V, Boolean>executeAndReturn(name, (map) -> map.replace(k.unwrap(), oldValue.unwrap(), newValue.unwrap()));
  }

  @Override
  public <K, V> void mapClear(String name) {
    logDebug(() -> "RpcMapService.clear name = [" + name + "]");
    this.<K, V>execute(name, (map) -> map.clear());
  }

  @Override
  public <K, V> void mapPutAll(String name, Map<DataHolder<K>, DataHolder<V>> m) {
    logDebug(() -> "RpcMapService.mapPutAll name = [" + name + "]");
    this.execute(name, (map) -> m.forEach((k, v) -> map.put(k.unwrap(), v.unwrap())));
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
