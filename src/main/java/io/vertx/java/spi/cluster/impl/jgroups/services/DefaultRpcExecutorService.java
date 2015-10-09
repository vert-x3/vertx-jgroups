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

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.java.spi.cluster.impl.jgroups.support.DataHolder;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class DefaultRpcExecutorService implements RpcExecutorService, LambdaLogger {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRpcExecutorService.class);
  private static final Message.Flag[] JGROUPS_FLAGS = new Message.Flag[]{Message.Flag.NO_TOTAL_ORDER};

  private final Vertx vertx;
  private final RpcDispatcher dispatcher;

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

    try {
      NotifyingFuture<RspList<T>> notifyingFuture = this.<T>execute(action, options);
      RspList<T> rspList = notifyingFuture.get(timeout, TimeUnit.MILLISECONDS);
      return futureDone(rspList);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public <T> void remoteExecute(MethodCall action, Handler<AsyncResult<T>> handler) {
    this.remoteExecute(action, 0, handler);
  }

  @Override
  public <T> void remoteExecute(MethodCall action, long timeout, Handler<AsyncResult<T>> handler) {
    logTrace(() -> String.format("RemoteExecute action %s, handler %s", action, handler));
    RequestOptions options = new RequestOptions()
        .setFlags(JGROUPS_FLAGS)
        .setMode(ResponseMode.GET_ALL)
        .setTimeout(timeout);
    try {
      NotifyingFuture<RspList<T>> notifyingFuture = this.<T>execute(action, options);
      notifyingFuture.setListener((future) -> {
        try {
          RspList<T> rspList = future.get();
          T result = futureDone(rspList);
          vertx.runOnContext(h -> handler.handle(Future.succeededFuture(result)));
        } catch (Exception e) {
          vertx.runOnContext(h -> handler.handle(Future.failedFuture(e)));
        }
      });
    } catch (Exception e) {
      vertx.runOnContext(h -> handler.handle(Future.failedFuture(e)));
    }
  }

  private <T> NotifyingFuture<RspList<T>> execute(MethodCall action, RequestOptions options) throws Exception {
    return this.<T>internalExecute(action, options);
  }

  private <T> NotifyingFuture<RspList<T>> internalExecute(MethodCall action, RequestOptions options) throws Exception {
    return dispatcher.<T>callRemoteMethodsWithFuture(null, action, options);
  }

  private <T> T futureDone(RspList<T> rspList) {
    Collection<Rsp<T>> values = rspList.values();

    values.parallelStream()
        .filter(Rsp::hasException)
        .forEach(rsp -> logWarn(() -> String.format("Execute method failed. Sender [%s], with exception [%s]", rsp.getSender(), rsp.getException())));

    T value = values.stream()
        .filter(((Predicate<Rsp<T>>) Rsp::wasUnreachable).or(Rsp::hasException).negate())
        .map(Rsp::getValue)
        .filter((t) -> t != null)
        .reduce((a, b) -> a)
        .orElse(null);

    if (value instanceof DataHolder) {
      return ((DataHolder<T>) value).unwrap();
    } else {
      return value;
    }
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
