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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.jgroups.JGroupsClusterManager;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class JGroupsClusteredEventbusTest extends ClusteredEventBusTest {

  private static final Logger log = LoggerFactory.getLogger(JGroupsClusteredEventbusTest.class);

  @Rule
  public JGroupsCleanupRule testingJGroups = new JGroupsCleanupRule();

  public static final String WRAPPED_CHANNEL = "wrapper-channel";
  private static final String ADDRESS1 = "some-address1";

  private static int VERTX_NODE_0 = 0;
  private static int VERTX_NODE_1 = 1;
  private static int VERTX_NODE_2 = 2;

  @Override
  protected ClusterManager getClusterManager() {
    return new JGroupsClusterManager();
  }

  @Test
  public void testSubsRemovedForKilledNode2() throws Exception {
    startNodes(2);

    AtomicInteger cnt = new AtomicInteger();

    vertices[VERTX_NODE_1].eventBus().consumer(WRAPPED_CHANNEL, wrapperEvent -> {
      log.info("Wrapper channel received request");
      EventBus ebSender = vertices[VERTX_NODE_1].eventBus();
      ebSender.send(ADDRESS1, "foo" + (int) wrapperEvent.body(), reply -> {
        assertEquals("ok", reply.result().body().toString().substring(0, 2));
        wrapperEvent.reply("ok");
      });
    });

    CountDownLatch regLatch = new CountDownLatch(1);
    vertices[VERTX_NODE_0].eventBus().consumer(ADDRESS1, msg -> {
      int c = cnt.getAndIncrement();
      assertEquals(msg.body(), "foo" + c);

      log.info(">> receive and reply in consumer before kill");

      msg.reply("ok" + c);

      if (c > 1) {
        fail("too many messages");
      }
    }).completionHandler(onSuccess(v -> {
      vertices[VERTX_NODE_1].runOnContext(v1 -> {
        // Now send some messages from node 2 - they should ALL go to node 0
        log.info("Send request through the wrapper");
        EventBus ebSender = vertices[VERTX_NODE_1].eventBus();
        ebSender.send(WRAPPED_CHANNEL, 0, handler -> {
          log.info("Received request through the wrapper");
          if (handler.succeeded()) {
            regLatch.countDown();
          }
        });
      });
    }));

    awaitLatch(regLatch);
    kill(VERTX_NODE_0);

    // Allow time for kill to be propagate
    Thread.sleep(2000);

    addNodes(1);

    Thread.sleep(500);

    CountDownLatch directCount = new CountDownLatch(1);
    vertices[VERTX_NODE_2].eventBus().consumer(ADDRESS1, msg -> {

      int c = cnt.getAndIncrement();
      assertEquals(msg.body(), "foo" + c);
      msg.reply("ok" + c);

      if (c == 0) {
        fail("should not get first messages");
      }
    }).completionHandler(onSuccess(v2 -> {
      //Check that this consumer available directly
      vertices[VERTX_NODE_1].runOnContext(v -> {
        EventBus ebSender = vertices[VERTX_NODE_1].eventBus();
        ebSender.send(ADDRESS1, "foo" + 1, replyHandler -> {
          log.info(">>> direct request is " + replyHandler.succeeded());
          if (replyHandler.failed()) {
            fail("I should receive success reply");
          }
          directCount.countDown();
        });
      });
    }));
    awaitLatch(directCount);

    CountDownLatch wrappedCount = new CountDownLatch(1);
    vertices[VERTX_NODE_2].runOnContext(v1 -> {

      log.info("Send request through the wrapper");
      EventBus ebSender = vertices[VERTX_NODE_2].eventBus();
      ebSender.send(WRAPPED_CHANNEL, 2, handler -> {
        log.info("Received request through the wrapper");
        if (handler.succeeded()) {
          wrappedCount.countDown();
        }
      });
    });
    awaitLatch(wrappedCount);
    complete();
  }

  private void removeNode(int pos) {
    Vertx[] tempVertices = new Vertx[vertices.length - 1];
    int j = 0;
    for (int i = 0; i < vertices.length;) {
      if(i!=pos) {
        tempVertices[j] = vertices[i];
        j++;
      }
      i++;
    }
    vertices = tempVertices;
  }


  protected void addNodes(int numNodes) {
    System.out.println("Before add node");

    addNodes(numNodes, getOptions());
  }

  protected void addNodes(int numNodes, VertxOptions options) {
    CountDownLatch latch = new CountDownLatch(numNodes);
    Vertx[] currentVertices = vertices;
    int totalVert = numNodes + vertices.length;
    vertices = new Vertx[totalVert];

    for (int i = 0; i < currentVertices.length; i++) {
      vertices[i] = currentVertices[i];
    }


    for (int i = currentVertices.length; i < totalVert; i++) {
      int index = i;
      Vertx.clusteredVertx(options.setClusterHost("localhost").setClusterPort(0).setClustered(true)
          .setClusterManager(getClusterManager()), ar -> {
        if (ar.failed()) {
          ar.cause().printStackTrace();
        }
        assertTrue("Failed to start node", ar.succeeded());
        vertices[index] = ar.result();
        latch.countDown();
      });
    }
    try {
      assertTrue(latch.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }

    log.info("Added new nodes count " + numNodes + ", total count " + vertices.length);
  }

  protected void kill(int pos) {
    VertxInternal v = (VertxInternal) vertices[pos];
    v.executeBlocking(fut -> {
      v.simulateKill();
      fut.complete();
    }, ar -> {
      assertTrue(ar.succeeded());
    });
  }
}
