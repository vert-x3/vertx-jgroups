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
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Rsp;

import java.util.Optional;
import java.util.function.Supplier;

public interface RpcExecutorService {

  <T> T remoteExecute(MethodCall action);

  <T> T remoteExecute(MethodCall action, long timeout);

  <T> void remoteExecute(MethodCall action, Handler<AsyncResult<T>> handler);

  <T> void remoteExecute(MethodCall action, long timeout, Handler<AsyncResult<T>> handler);

  <T> void asyncExecute(Supplier<T> action, Handler<AsyncResult<T>> handler);
}
