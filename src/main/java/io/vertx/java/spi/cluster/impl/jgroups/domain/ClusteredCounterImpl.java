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


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.jgroups.blocks.atomic.Counter;

public class ClusteredCounterImpl implements io.vertx.core.shareddata.Counter {

  private Vertx vertx;
  private Counter counter;

  public ClusteredCounterImpl(Vertx vertx, Counter counter) {
    this.vertx = vertx;
    this.counter = counter;
  }

  @Override
  public void get(Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking((future) -> future.complete(counter.get()), handler);
  }

  @Override
  public void incrementAndGet(Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking((future) -> future.complete(counter.incrementAndGet()), handler);
  }

  @Override
  public void getAndIncrement(Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking((future) -> future.complete(counter.incrementAndGet() - 1), handler);
  }

  @Override
  public void decrementAndGet(Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking((future) -> future.complete(counter.decrementAndGet()), handler);
  }

  @Override
  public void addAndGet(long delta, Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking((future) -> future.complete(counter.addAndGet(delta)), handler);
  }

  @Override
  public void getAndAdd(long delta, Handler<AsyncResult<Long>> handler) {
    vertx.executeBlocking(
        (future) -> future.complete(counter.addAndGet(delta) - delta),
        handler
    );
  }

  @Override
  public void compareAndSet(long expected, long update, Handler<AsyncResult<Boolean>> handler) {
    vertx.executeBlocking(
        (future) -> future.complete(counter.compareAndSet(expected, update)),
        handler
    );
  }

}
