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

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.jgroups.JGroupsClusterManager;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

public class JGroupsClusterWideMapTest extends ClusterWideMapTestDifferentNodes {

  @Rule
  public JGroupsCleanupRule testingJGroups = new JGroupsCleanupRule(true);

  @Override
  protected ClusterManager getClusterManager() {
    return new JGroupsClusterManager();
  }

  @Override
  @Test
  public void testMapPutTtl() {
    getVertx().sharedData().getClusterWideMap("unsupported", onSuccess(map -> {
      map.put("k", "v", 13, onFailure(cause -> {
        assertThat(cause, instanceOf(UnsupportedOperationException.class));
        testComplete();
      }));
    }));
    await();
  }

  @Override
  @Test
  public void testMapPutIfAbsentTtl() {
    getVertx().sharedData().getClusterWideMap("unsupported", onSuccess(map -> {
      map.putIfAbsent("k", "v", 13, onFailure(cause -> {
        assertThat(cause, instanceOf(UnsupportedOperationException.class));
        testComplete();
      }));
    }));
    await();
  }
}
