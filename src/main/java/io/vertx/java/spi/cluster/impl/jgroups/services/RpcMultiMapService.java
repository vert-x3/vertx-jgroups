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

import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;

public interface RpcMultiMapService {

  boolean multiMapCreate(String name);

  <K, V> void multiMapAdd(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> boolean multiMapRemove(String name, DataHolder<K> k, DataHolder<V> v);

  <K, V> void multiMapRemoveAll(String name, DataHolder<V> v);
}
