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

package io.vertx.spi.cluster.jgroups.impl.services;

import io.vertx.spi.cluster.jgroups.impl.support.DataHolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface RpcMapService {

  <K, V> Map<K, V> mapCreate(String name);

  <K, V> void mapPut(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> DataHolder<V> mapPutIfAbsent(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> DataHolder<V> mapRemove(String name, DataHolder<K> k);

  <K, V> boolean mapRemoveIfPresent(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> DataHolder<V> mapReplace(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> boolean mapReplaceIfPresent(String name, DataHolder<K> k, DataHolder<V> oldValue, DataHolder<V> newValue);

  <K, V> void mapClear(String name);

  <K, V> void mapPutAll(String name, Map<DataHolder<K>, DataHolder<V>> m);

  void writeTo(OutputStream output) throws IOException;

  void readFrom(InputStream input) throws IOException, ClassNotFoundException;
}
