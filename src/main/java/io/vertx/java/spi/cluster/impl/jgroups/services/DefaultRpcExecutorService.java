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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;
import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultRpcExecutorService implements RpcExecutorService, LambdaLogger {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRpcExecutorService.class);
  private static final Message.Flag[] JGROUPS_FLAGS = new Message.Flag[]{Message.Flag.NO_TOTAL_ORDER};

  private final Vertx vertx;
  private final RpcDispatcher dispatcher;
  private boolean active = true;

  public DefaultRpcExecutorService(Vertx vertx, RpcDispatcher dispatcher) {
    this.vertx = vertx;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T> T remoteExecute(MethodCall action, long timeout) {
    logTrace(() -> String.format("RemoteExecute sync action %s with timeout %s", action, timeout));
    RequestOptions options = new RequestOptions()
        .setFlags(JGROUPS_FLAGS)
        .setMode(ResponseMode.GET_MAJORITY)
        .setTimeout(timeout);
    return execute(action, options);
  }

  @Override
  public <T> void remoteExecute(MethodCall action, Handler<AsyncResult<T>> handler) {
    this.<T>remoteExecute(action, 0, handler);
  }

  @Override
  public <T> void remoteExecute(MethodCall action, long timeout, Handler<AsyncResult<T>> handler) {
    logTrace(() -> String.format("RemoteExecute action %s, handler %s", action, handler));
    RequestOptions options = new RequestOptions()
        .setFlags(JGROUPS_FLAGS)
        .setMode(ResponseMode.GET_ALL)
        .setTimeout(timeout);
    this.<T>asyncExecute(() -> this.<T>execute(action, options), handler);
  }

  @Override
  public <T> void asyncExecute(Supplier<T> action, Handler<AsyncResult<T>> handler) {
    logTrace(() -> String.format("AsyncExecute action %s, handler %s", action.toString(), handler.toString()));
    vertx.executeBlocking((event) -> event.complete(action.get()), handler);
  }

  @Override
  public void stop() {
    active = false;
  }

  private <T> T execute(MethodCall action, RequestOptions options) {
    if (!active) {
      logError(() -> "Cannot execute remote dispatch from inactive nodes");
      throw new VertxException("Cannot execute remote dispatch from inactive nodes");
    }

    Collection<Rsp<Object>> rsps = this.internalExecute(action, options);

    logTrace(
        () -> rsps.stream().filter(Rsp::hasException),
        rsp -> String.format("Execute method [%s] failed. Sender [%s], with exception [%s]", action, rsp.getSender(), rsp.getException()));

    List<Object> list = rsps.stream()
        .filter(((Predicate<Rsp<Object>>) Rsp::hasException).negate())
        .map(Rsp::getValue)
        .collect(Collectors.toList());
    if (list == null || list.isEmpty()) {
      return null;
    }

    Object object = list.get(0);
    logTrace(() -> String.format("Remote execute -> Response [%s]", object));
    if (object instanceof DataHolder) {
      logTrace(() -> String.format("Remote execute -> Unwrap response to data [%s]", object));
      return ((DataHolder<T>) object).unwrap();
    } else {
      return (T) object;
    }
  }

  private <T> Collection<Rsp<T>> internalExecute(MethodCall action, RequestOptions options) {
    logTrace(() -> String.format("Execute action [%s]", action.toStringDetails()));
    try {
      return this.<T>broadDispatch(action, options).values();
    } catch (Exception e) {
      logError(() -> String.format("Execute action [%s]", action.toStringDetails()));
      throw new VertxException(e);
    }
  }

  private <T> RspList<T> broadDispatch(MethodCall action, RequestOptions options) throws Exception {
    logTrace(() -> String.format("BroadDispatch action [%s] - options [%s]", action.toStringDetails(), options.toString()));
    return dispatcher.callRemoteMethods(null, action, options);
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
