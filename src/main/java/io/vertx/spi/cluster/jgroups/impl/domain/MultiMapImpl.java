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

package io.vertx.spi.cluster.jgroups.impl.domain;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.spi.cluster.jgroups.impl.support.LambdaLogger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class MultiMapImpl<K, V> implements MultiMap<K,V>, LambdaLogger {

  private final static Logger log = LoggerFactory.getLogger(MultiMapImpl.class);
  private String name;

  private Map<K, ChoosableArrayList<V>> cache = new ConcurrentHashMap<>();

  public MultiMapImpl() {
  }

  public MultiMapImpl(String name) {
    this.name = name;
  }

  @Override
  public void add(K k, V v) {
    logTrace(() -> String.format("MultiMapImpl.add name = [%s] and  k = [%s], v = [%s]", name, k, v));
    cache.compute(k, (key, oldValue) ->
            Optional.ofNullable(oldValue)
                .orElseGet(() -> ChoosableArrayList.emptyChoosable)
                .add(v)
    );
  }

  @Override
  public ChoosableArrayList<V> get(K k) {
    ChoosableArrayList<V> v = cache.getOrDefault(k, ChoosableArrayList.emptyChoosable);
    logTrace(() -> String.format("MultiMapImpl.get name = [%s] and  k = [%s], v = [%s]", name, k, v));
    return v;
  }

  @Override
  public boolean remove(K k, V v) {
    logTrace(() -> String.format("MultiMapImpl.remove name = [%s] and  k = [%s], v = [%s]", name, k, v));
    final boolean[] result = {false};
    cache.computeIfPresent(k, (key, oldValue) -> {
      result[0] = true;
      return oldValue.remove(v);
    });
    return result[0];
  }

  @Override
  public void removeAll(V v) {
    logTrace(() -> String.format("MultiMapImpl.removeAll name = [%s] and  v = [%s]", name, v));
    cache.replaceAll((k, oldValue) -> oldValue.remove(v));
  }

  @Override
  public void removeAllMatching(Predicate<V> p) {
    logTrace(() -> String.format("MultiMapImpl.removeAllMatching name = [%s]", name));
    cache.replaceAll((k, oldValue) -> {
      for (Iterator<V> iterator = oldValue.iterator(); iterator.hasNext(); ) {
        V v = iterator.next();
        if (p.test(v)) {
          iterator.remove();
        }
      }
      return oldValue;
    });
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    logTrace(() -> String.format("MultiMapImpl.writeExternal name = [%s] and  cache = {%s}", name, cache));
    out.writeInt(cache.size());
    out.writeUTF(name);
    for (Map.Entry<K, ChoosableArrayList<V>> entry : cache.entrySet()) {
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
      ChoosableArrayList<V> value = (ChoosableArrayList<V>) in.readObject();
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
