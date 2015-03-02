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
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.blocks.locking.LockService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class ClusteredLockImpl implements io.vertx.core.shareddata.Lock, LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(ClusteredLockImpl.class);
  private final Lock lock;

  public ClusteredLockImpl(LockService lockService, String name) {
    lock = lockService.getLock(name);
  }

  public boolean acquire(long timeout) {
    try {
      logDebug(() -> String.format("Acquire lock on [%s] with timeout [%d]", lock, timeout));
      return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
    return false;
  }

  @Override
  public void release() {
    lock.unlock();
  }

  @Override
  public Logger log() {
    return LOG;
  }
}
