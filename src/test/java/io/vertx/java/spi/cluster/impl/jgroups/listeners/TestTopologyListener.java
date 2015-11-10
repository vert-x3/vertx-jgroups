package io.vertx.java.spi.cluster.impl.jgroups.listeners;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.impl.launcher.commands.ClasspathHandler;
import io.vertx.core.spi.cluster.NodeListener;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestTopologyListener {

  @Mock
  private Vertx vertx;

  @Mock
  private NodeListener nodeListener;

  @Test
  public void testNodeAdded() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);

    UUID creator = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));

    topologyListener.viewAccepted(new View(creator, 1, Arrays.asList(node1, node2)));

    verify(vertx, times(2)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    topologyListener.viewAccepted(new View(creator, 2, Arrays.asList(node1, node2, node3)));

    verify(vertx, times(1)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getValue().handle(null);

    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));
  }

  @Test
  public void testNodeLeft() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    reset(vertx, nodeListener);

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);

    UUID creator = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));

    topologyListener.viewAccepted(new View(creator, 1, Arrays.asList(node1, node2, node3)));

    verify(vertx, times(3)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    topologyListener.viewAccepted(new View(creator, 2, Arrays.asList(node1, node3)));

    verify(vertx, times(1)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getValue().handle(null);

    verify(nodeListener, times(1)).nodeLeft(eq(node2.toString()));
  }


  @Test
  public void testNodeAddedAndLeft() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    reset(vertx, nodeListener);

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);

    UUID creator = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node4 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));

    topologyListener.viewAccepted(new View(creator, 1, Arrays.asList(node1, node2, node3)));

    verify(vertx, times(3)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));
    verify(nodeListener, never()).nodeLeft(anyString());

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    topologyListener.viewAccepted(new View(creator, 2, Arrays.asList(node1, node3, node4)));

    verify(vertx, times(2)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeLeft(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node4.toString()));
  }

  @Test
  public void testNodeAllLeft() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    reset(vertx, nodeListener);

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);

    UUID creator = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));

    topologyListener.viewAccepted(new View(creator, 1, Arrays.asList(creator, node1, node2, node3)));

    verify(vertx, times(4)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(creator.toString()));
    verify(nodeListener, never()).nodeLeft(anyString());

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    topologyListener.viewAccepted(new View(creator, 2, Arrays.asList(creator)));

    verify(vertx, times(3)).<Void>executeBlocking(captorHandler.capture(), any());

    System.out.println(captorHandler.getAllValues().toString());
    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeLeft(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeLeft(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeLeft(eq(node3.toString()));
    verify(nodeListener, never()).nodeAdded(anyString());
  }

  @Test
  public void testNodeAddExistingAddress() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    reset(vertx, nodeListener);

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);

    UUID creator = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));

    topologyListener.viewAccepted(new View(creator, 1, Arrays.asList(creator, node1, node2, node3)));

    verify(vertx, times(4)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(creator.toString()));
    verify(nodeListener, never()).nodeLeft(anyString());

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    topologyListener.viewAccepted(new View(creator, 2, Arrays.asList(creator, node1, node2, node3, node1)));

    verify(vertx, never()).<Void>executeBlocking(captorHandler.capture(), any());
  }

  @Test
  public void testNodeRealView() {
    ArgumentCaptor<Handler<Future<Void>>> captorHandler;
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

    reset(vertx, nodeListener);

    TopologyListener topologyListener = new TopologyListener(vertx);
    topologyListener.setNodeListener(nodeListener);


//    [[localhost-7084|9] (6) [localhost-7084, localhost-25611, localhost-15358, localhost-55039, localhost-31903, localhost-31638]]
    UUID node1 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node2 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node3 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node4 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node5 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID node6 = new UUID((long)(Math.random()*Long.MAX_VALUE), (long)(Math.random()*Long.MAX_VALUE));
    UUID creator = node1;

    topologyListener.viewAccepted(new View(creator, 9, Arrays.asList(node1, node2, node3, node4, node5, node6)));

    verify(vertx, times(6)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, times(1)).nodeAdded(eq(node1.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node2.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node3.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node4.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node5.toString()));
    verify(nodeListener, times(1)).nodeAdded(eq(node6.toString()));
    verify(nodeListener, never()).nodeLeft(anyString());

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

//    [[localhost-25611|10] (5) [localhost-25611, localhost-15358, localhost-55039, localhost-31903, localhost-31638]]

    creator = node3;

    topologyListener.viewAccepted(new View(creator, 10, Arrays.asList(node2, node3, node4, node5, node6)));

    verify(vertx, times(1)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, never()).nodeAdded(anyString());
    verify(nodeListener, times(1)).nodeLeft(eq(node1.toString()));

    reset(vertx, nodeListener);
    captorHandler = new ArgumentCaptor<Handler<Future<Void>>>();

//    [[localhost-25611|11] (4) [localhost-25611, localhost-55039, localhost-31903, localhost-31638]]
    creator = node3;

    topologyListener.viewAccepted(new View(creator, 11, Arrays.asList(node3, node4, node5, node6)));

    verify(vertx, times(1)).<Void>executeBlocking(captorHandler.capture(), any());

    captorHandler.getAllValues().stream().forEach((h) -> h.handle(null));

    verify(nodeListener, never()).nodeAdded(anyString());
    verify(nodeListener, times(1)).nodeLeft(eq(node2.toString()));
  }


}
