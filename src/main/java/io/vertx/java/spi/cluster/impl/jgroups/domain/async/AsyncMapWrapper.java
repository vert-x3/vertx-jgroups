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

package io.vertx.java.spi.cluster.impl.jgroups.domain.async;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.java.spi.cluster.impl.jgroups.services.RpcExecutorService;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;

import java.util.Map;

import static io.vertx.java.spi.cluster.impl.jgroups.services.RpcServerObjDelegate.*;

public class AsyncMapWrapper<K, V> implements AsyncMap<K, V>, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(AsyncMapWrapper.class);

  private final String name;
  private final Map<K, V> map;
  private final RpcExecutorService executorService;

  public AsyncMapWrapper(String name, Map<K, V> map, RpcExecutorService executorService) {
    this.name = name;
    this.map = map;
    this.executorService = executorService;
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> handler) {
    logTrace(() -> "get k = [" + k + "], handler = [" + handler + "]");
    executorService.runAsync(() -> map.get(k), handler);
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> handler) {
    logTrace(() -> "put k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_PUT.method(name, k, v), handler);
  }

  @Override
  public void put(K k, V v, long timeout, Handler<AsyncResult<Void>> handler) {
    logTrace(() -> "put k = [" + k + "], v = [" + v + "], timeout = [" + timeout + "] handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_PUT.method(name, k, v), timeout, handler);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> handler) {
    logTrace(() -> "putIfAbsent k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_PUTIFABSENT.method(name, k, v), handler);
  }

  @Override
  public void putIfAbsent(K k, V v, long timeout, Handler<AsyncResult<V>> handler) {
    logTrace(() -> "putIfAbsent k = [" + k + "], v = [" + v + "], timeout = [" + timeout + "] handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_PUTIFABSENT.method(name, k, v), timeout, handler);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> handler) {
    logTrace(() -> "remove k = [" + k + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_REMOVE.method(name, k), handler);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> handler) {
    logTrace(() -> "removeIfPresent k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_REMOVEIFPRESENT.method(name, k, v), handler);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> handler) {
    logTrace(() -> "replace k = [" + k + "], v = [" + v + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_REPLACE.method(name, k, v), handler);
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> handler) {
    logTrace(() -> "replaceIfPresent k = [" + k + "], oldValue = [" + oldValue + "], newValue = [" + newValue + "], handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_REPLACEIFPRESENT.method(name, k, oldValue, newValue), handler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> handler) {
    logTrace(() -> "clear handler = [" + handler + "]");
    executorService.remoteExecute(CALL_MAP_CLEAR.method(name), handler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> handler) {
    logTrace(() -> "size handler = [" + handler + "]");
    executorService.runAsync(map::size, handler);
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
