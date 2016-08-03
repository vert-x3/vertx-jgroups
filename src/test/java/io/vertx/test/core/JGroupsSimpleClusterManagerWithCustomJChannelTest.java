/*
 * Copyright (c) 2011-2014 The original author or authors
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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.jgroups.JGroupsClusterManager;

import org.jgroups.JChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Clement Escoffier - clement@apache.org
 */
public class JGroupsSimpleClusterManagerWithCustomJChannelTest extends AsyncTestBase {

  private JChannel channel1;
  private JChannel channel2;

  @Before
  public void setupJGroups() throws Exception {
    try (InputStream stream1 = JGroupsClusterManager.getConfigStream()) {
      channel1 = new JChannel(stream1);
    }
    try (InputStream stream2 = JGroupsClusterManager.getConfigStream()) {
      channel2 = new JChannel(stream2);
    }
  }

  @After
  public void tearDownJGroups() throws Exception {
    Assert.assertTrue( "Failed to close some testing channels", closeChannel(channel1) && closeChannel(channel2) );
  }

  private static boolean closeChannel(final JChannel someChannel) {
    if (someChannel!=null) {
      try {
        someChannel.close();
      }
      catch (Throwable e) {
        //This is interesting but allow closing all channels first
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  @Test
  public void testEventBusP2P() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager(channel1);
    JGroupsClusterManager mgr2 = new JGroupsClusterManager(channel2);

    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClustered(true).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getNodeID());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        testComplete();
      });
      vertx1.set(res.result());
    });

    waitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getNodeID());
      vertx2.set(res.result());
      res.result().eventBus().send("news", "hello");
    });

    await();
    vertx1.get().close();
    vertx2.get().close();
  }

  @Test
  public void testEventBusPubSub() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager(channel1);
    JGroupsClusterManager mgr2 = new JGroupsClusterManager(channel2);

    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClustered(true).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    AtomicInteger counter = new AtomicInteger();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getNodeID());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        counter.incrementAndGet();
      });
      vertx1.set(res.result());
    });

    waitUntil(() -> vertx1.get() != null);

    // Producer
    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getNodeID());
      vertx2.set(res.result());
      res.result().eventBus().publish("news", "hello");
    });

    waitUntil(() -> counter.get() == 1);
    vertx1.get().close();
    vertx2.get().close();
  }

  @Test
  public void testEventBusWithReply() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager(channel1);
    JGroupsClusterManager mgr2 = new JGroupsClusterManager(channel2);

    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClustered(true).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getNodeID());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("ping"));
        message.reply("pong");
      });
      vertx1.set(res.result());
    });

    waitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getNodeID());
      vertx2.set(res.result());
      res.result().eventBus().send("news", "ping", ar -> {
        if (ar.succeeded()) {
          assertTrue(ar.result().body().equals("pong"));
          testComplete();
        }
      });
    });

    await();
    vertx1.get().close();
    vertx2.get().close();
  }

  @Test
  public void testSharedData() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager(channel1);
    JGroupsClusterManager mgr2 = new JGroupsClusterManager(channel2);

    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClustered(true).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getNodeID());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    waitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getNodeID());
      vertx2.set(res.result());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().get("news", r -> {
          assertEquals("hello", r.result());
          testComplete();
        });
      });
    });

    await();
    vertx1.get().close();
    vertx2.get().close();
  }
}
