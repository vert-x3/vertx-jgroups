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
package io.vertx.test.core;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.jgroups.Address;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.util.AsciiString;
import org.junit.Assert;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Sanne Grinovero
 */
public class JGroupsCleanupRule implements TestRule {

  private static final ConcurrentMap<AsciiString,Map<Address,SHARED_LOOPBACK>> routing_table = fetchRoutingTable();

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        preconditionsAreClean();
        try {
          base.evaluate();
        } finally {
          forceCleanup();
        }
      }
    };
  }

  protected void forceCleanup() {
    final int size = routing_table.size();
    routing_table.clear();
    if (size != 0) {
      Assert.fail("Test didn't close all JGroups channels. Forcing a reset of the SHARED_LOOPBACK#routing_table to avoid affecting other tests.");
    }
  }

  private static ConcurrentMap<AsciiString, Map<Address, SHARED_LOOPBACK>> fetchRoutingTable() {
    try {
      Field field = SHARED_LOOPBACK.class.getDeclaredField("routing_table");
      field.setAccessible(true);
      return (ConcurrentMap<AsciiString, Map<Address, SHARED_LOOPBACK>>) field.get(null);
    }
    catch (Exception e) {
      //This will either work, or the class initialization will have broken and no single test is going to pass.
      e.printStackTrace();
      return null;
    }
  }

  protected void preconditionsAreClean() {
    // This is the only way to inspect the SHARED_LOOPBACK without resorting to reflection:
    String dumpRoutingTable = SHARED_LOOPBACK.dumpRoutingTable();
    if ( ! "".equals(dumpRoutingTable)) {
      Assert.fail("Preconditions for test failed. Refusing to start a JGroups test: routing_table of SHARED_LOOPBACK protocol is not clean!");
    }
  }

}
