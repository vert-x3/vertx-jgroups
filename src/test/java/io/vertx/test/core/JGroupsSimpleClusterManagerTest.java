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
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Clement Escoffier - clement@apache.org
 */
public class JGroupsSimpleClusterManagerTest extends AsyncTestBase {

  @Test
  public void testEventBusP2P() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager();
    JGroupsClusterManager mgr2 = new JGroupsClusterManager();

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
    JGroupsClusterManager mgr1 = new JGroupsClusterManager();
    JGroupsClusterManager mgr2 = new JGroupsClusterManager();
    JGroupsClusterManager mgr3 = new JGroupsClusterManager();
    JGroupsClusterManager mgr4 = new JGroupsClusterManager();

    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options3 = new VertxOptions().setClusterManager(mgr3).setClustered(true).setClusterHost("127.0.0.1");
    VertxOptions options4 = new VertxOptions().setClusterManager(mgr4).setClustered(true).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();
    AtomicReference<Vertx> vertx3 = new AtomicReference<>();
    AtomicReference<Vertx> vertx4 = new AtomicReference<>();

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

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getNodeID());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        counter.incrementAndGet();
      });
      vertx2.set(res.result());
    });

    waitUntil(() -> vertx2.get() != null);

    Vertx.clusteredVertx(options3, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr3.getNodeID());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        counter.incrementAndGet();
      });
      vertx3.set(res.result());
    });

    waitUntil(() -> vertx3.get() != null);

    // Producer
    Vertx.clusteredVertx(options4, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr4.getNodeID());
      vertx4.set(res.result());
      res.result().eventBus().publish("news", "hello");
    });

    waitUntil(() -> counter.get() == 3);
    vertx1.get().close();
    vertx2.get().close();
    vertx3.get().close();
    vertx4.get().close();
  }

  @Test
  public void testEventBusWithReply() throws Exception {
    JGroupsClusterManager mgr1 = new JGroupsClusterManager();
    JGroupsClusterManager mgr2 = new JGroupsClusterManager();

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
    JGroupsClusterManager mgr1 = new JGroupsClusterManager();
    JGroupsClusterManager mgr2 = new JGroupsClusterManager();

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
