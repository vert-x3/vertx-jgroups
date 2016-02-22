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

package io.vertx.spi.cluster.jgroups.impl.domain.async;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.spi.cluster.jgroups.impl.domain.MultiMap;
import io.vertx.spi.cluster.jgroups.impl.services.RpcExecutorService;
import io.vertx.spi.cluster.jgroups.impl.support.LambdaLogger;
import io.vertx.spi.cluster.jgroups.impl.services.RpcServerObjDelegate;

public class AsyncMultiMapWrapper<K, V> implements AsyncMultiMap<K, V>, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(AsyncMultiMapWrapper.class);

  private final String name;
  private final MultiMap<K, V> map;
  private final RpcExecutorService executorService;

  public AsyncMultiMapWrapper(String name, MultiMap<K, V> map, RpcExecutorService executorService) {
    this.name = name;
    this.map = map;
    this.executorService = executorService;
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> handler) {
    logTrace(() -> "add k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(RpcServerObjDelegate.CALL_MULTIMAP_ADD.method(name, k, v), handler);
  }

  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> handler) {
    logTrace(() -> "get k = [" + k + "], handler = [" + handler + "]");
    executorService.runAsync(() -> map.get(k), handler);
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> handler) {
    logTrace(() -> "remove k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(RpcServerObjDelegate.CALL_MULTIMAP_REMOVE.method(name, k, v), handler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> handler) {
    executorService.remoteExecute(RpcServerObjDelegate.CALL_MULTIMAP_REMOVE_ALL.method(name, v), handler);
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
