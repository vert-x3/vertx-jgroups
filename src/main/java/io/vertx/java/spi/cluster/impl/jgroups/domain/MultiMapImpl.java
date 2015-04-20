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
import io.vertx.core.logging.impl.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MultiMapImpl<K, V> implements Externalizable {

  private final static Logger log = LoggerFactory.getLogger(MultiMapImpl.class);

  private Map<K, ImmutableChoosableSet<V>> cache = new ConcurrentHashMap<>();

  public MultiMapImpl() {
  }

  public void add(K k, V v) {
    if (log.isTraceEnabled()) {
      log.trace("MultiMapImpl.add k = [" + k + "], v = [" + v + "]");
    }
    cache.compute(k, (key, oldValue) ->
            Optional.ofNullable(oldValue)
                .orElseGet(() -> ImmutableChoosableSet.emptySet)
                .add(v)
    );
  }

  public ImmutableChoosableSet<V> get(K k) {
    ImmutableChoosableSet<V> v = cache.get(k);
    if (log.isTraceEnabled()) {
      log.trace("MultiMapImpl.get k = [" + k + "], v = [" + v + "]");
    }
    return v;
  }

  public boolean remove(K k, V v) {
    if (log.isTraceEnabled()) {
      log.trace("MultiMapImpl.remove k = [" + k + "], v = [" + v + "]");
    }
    final boolean[] result = {false};
    cache.computeIfPresent(k, (key, oldValue) -> {
      result[0] = true;
      return oldValue.remove(v);
    });
    return result[0];
  }

  public void removeAll(V v) {
    if (log.isTraceEnabled()) {
      log.trace("MultiMapImpl.removeAll v = [" + v + "]");
    }
    cache.replaceAll((k, oldValue) -> oldValue.remove(v));
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(cache.size());
    for (Map.Entry<K, ImmutableChoosableSet<V>> entry : cache.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      K key = (K) in.readObject();
      ImmutableChoosableSet<V> value = (ImmutableChoosableSet<V>) in.readObject();
      cache.put(key, value);
    }
  }

  @Override
  public String toString() {
    return "MultiMapImpl{" +
        "cache=" + cache +
        '}';
  }
}
