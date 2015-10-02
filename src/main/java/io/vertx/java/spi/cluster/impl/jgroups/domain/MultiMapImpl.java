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

package io.vertx.java.spi.cluster.impl.jgroups.domain;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MultiMapImpl<K, V> implements Externalizable, LambdaLogger {

  private final static Logger log = LoggerFactory.getLogger(MultiMapImpl.class);
  private String name;

  private Map<K, ImmutableChoosableSet<V>> cache = new ConcurrentHashMap<>();

  public MultiMapImpl() {
  }

  public MultiMapImpl(String name) {
    this.name = name;
  }

  public void add(K k, V v) {
    logTrace(() -> String.format("MultiMapImpl.add name = [%s] and  k = [%s], v = [%s]", name, k, v));
    cache.compute(k, (key, oldValue) ->
            Optional.ofNullable(oldValue)
                .orElseGet(() -> ImmutableChoosableSet.emptySet)
                .add(v)
    );
  }

  public ImmutableChoosableSet<V> get(K k) {
    ImmutableChoosableSet<V> v = cache.get(k);
    logTrace(() -> String.format("MultiMapImpl.get name = [%s] and  k = [%s], v = [%s]", name, k, v));
    return v;
  }

  public boolean remove(K k, V v) {
    logTrace(() -> String.format("MultiMapImpl.remove name = [%s] and  k = [%s], v = [%s]", name, k, v));
    final boolean[] result = {false};
    cache.computeIfPresent(k, (key, oldValue) -> {
      result[0] = true;
      return oldValue.remove(v);
    });
    return result[0];
  }

  public void removeAll(V v) {
    logTrace(() -> String.format("MultiMapImpl.removeAll name = [%s] and  v = [%s]", name, v));
    cache.replaceAll((k, oldValue) -> oldValue.remove(v));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    logTrace(() -> String.format("MultiMapImpl.writeExternal name = [%s] and  cache = {%s}", name, cache));
    out.writeInt(cache.size());
    out.writeUTF(name);
    for (Map.Entry<K, ImmutableChoosableSet<V>> entry : cache.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    logTrace(() -> String.format("MultiMapImpl.readExternal name = [%s] and  cache = {%s}", name, cache));
    int size = in.readInt();
    name = in.readUTF();
    for (int i = 0; i < size; i++) {
      K key = (K) in.readObject();
      ImmutableChoosableSet<V> value = (ImmutableChoosableSet<V>) in.readObject();
      cache.put(key, value);
    }
  }

  @Override
  public String toString() {
    return "MultiMapImpl{" +
        "name=" + name + "," +
        "cache=" + cache +
        '}';
  }

  @Override
  public Logger log() {
    return log;
  }
}
