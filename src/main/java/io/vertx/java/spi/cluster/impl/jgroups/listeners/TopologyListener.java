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

package io.vertx.java.spi.cluster.impl.jgroups.listeners;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.*;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TopologyListener extends ReceiverAdapter implements LambdaLogger {

  private final static Logger LOG = LoggerFactory.getLogger(TopologyListener.class);
  private final Vertx vertx;
  private Address address;

  private ViewId viewId;
  private List<String> members = Collections.emptyList();
  private Optional<NodeListener> nodeListener = Optional.empty();

  public TopologyListener(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void receive(Message msg) {
    logTrace(() -> String.format("Message receive [%s]", msg));
  }

  @Override
  public void viewAccepted(View view) {
    logDebug(() -> String.format("Called View accepted [%s]", view));

    if (view.getViewId() == null) {
      logTrace(() -> String.format("Called View accepted [%s] with ViewId null.", view));
      return;
    }

    if (viewId != null && view.getViewId().compareToIDs(viewId) <= 0) {
      logTrace(() -> String.format("Called View accepted [%s] but there's no changes.", view));
      return;
    }

    List oldMembers = members;
    members = view.getMembers().stream()
        .map(Address::toString)
        .collect(Collectors.toList());
    viewId = view.getViewId().copy();
    nodeListener.ifPresent(listener -> notifyNodeListener(listener, members, oldMembers));
  }

  private void notifyNodeListener(NodeListener listener, List<String> newMembers, List<String> oldMembers) {
    if (newMembers == null) {
      return;
    }

    vertx.<Void>executeBlocking((event) -> {
      newMembers.stream()
          .filter(((Predicate<String>) oldMembers::contains).negate())
          .peek((member) -> logDebug(() -> String.format("Notify node [%s] that node [%s] has joined the cluster", address, member)))
          .forEach(listener::nodeAdded);

      oldMembers.stream()
          .filter(((Predicate<String>) newMembers::contains).negate())
          .peek((member) -> logDebug(() -> String.format("Notify node [%s] that node [%s] has left the cluster", address, member)))
          .forEach(listener::nodeLeft);

      event.complete();
    }, (v) -> Function.identity());
  }

  public void setNodeListener(NodeListener nodeListener) {
    logDebug(() -> String.format("Set topology listener [%s]", nodeListener));
    this.nodeListener = Optional.of(nodeListener);
  }

  public List<String> getNodes() {
    logDebug(() -> String.format("Get Nodes from topology [%s]", members));
    return Collections.unmodifiableList(members);
  }

  @Override
  public Logger log() {
    return LOG;
  }

  public void setAddress(Address address) {
    this.address = address;
  }
}
