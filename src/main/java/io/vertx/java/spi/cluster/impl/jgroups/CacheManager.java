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

package io.vertx.java.spi.cluster.impl.jgroups;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.java.spi.cluster.impl.jgroups.domain.MultiMapImpl;
import io.vertx.java.spi.cluster.impl.jgroups.domain.SyncMapWrapper;
import io.vertx.java.spi.cluster.impl.jgroups.domain.async.AsyncMapWrapper;
import io.vertx.java.spi.cluster.impl.jgroups.domain.async.AsyncMultiMapWrapper;
import io.vertx.java.spi.cluster.impl.jgroups.services.*;
import io.vertx.java.spi.cluster.impl.jgroups.support.LambdaLogger;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.RpcDispatcher;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheManager extends ReceiverAdapter implements LambdaLogger {

  private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

  private final Vertx vertx;
  private JChannel channel;
  private final RpcDispatcher dispatcher;

  private final Map<String, Map> maps = new ConcurrentHashMap<>();
  private final Map<String, MultiMapImpl> multiMaps = new ConcurrentHashMap<>();

  private final RpcExecutorService executorService;
  private final RpcMultiMapService multiMapService;
  private final RpcMapService mapService;

  public CacheManager(Vertx vertx, JChannel channel) {
    this.vertx = vertx;
    this.channel = channel;

    this.multiMapService = new DefaultRpcMultiMapService(multiMaps);
    this.mapService = new DefaultRpcMapService(maps);

    RpcServerObjDelegate server_obj = new RpcServerObjDelegate(mapService, multiMapService);
    // Don't want to loose the channel receiver.
    this.dispatcher = new RpcDispatcher(this.channel, this, channel.getReceiver(), server_obj);
    this.dispatcher.setMethodLookup(server_obj.getMethodLookup());

    this.executorService = new DefaultRpcExecutorService(vertx, dispatcher);
  }

  public <K, V> void createAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> handler) {
    logDebug(() -> String.format("method createAsyncMultiMap address[%s] name[%s]", channel.getAddressAsString(), name));
    vertx.executeBlocking(
        future -> {
          multiMapService.multiMapCreate(name);
          future.complete(new AsyncMultiMapWrapper<K, V>(name, multiMaps.get(name), executorService));
        },
        handler
    );
  }

  public <K, V> void createAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> handler) {
    logDebug(() -> String.format("method createAsyncMap address[%s] name[%s]", channel.getAddressAsString(), name));
    vertx.executeBlocking(
        future -> {
          mapService.mapCreate(name);
          future.complete(new AsyncMapWrapper<K, V>(name, maps.get(name), executorService));
        },
        handler
    );
  }

  public <K, V> Map<K, V> createSyncMap(String name) {
    logDebug(() -> String.format("method createSyncMap address[%s] name[%s]", channel.getAddressAsString(), name));
    mapService.mapCreate(name);
    return new SyncMapWrapper<>(name, maps.get(name), executorService);
  }

  @Override
  public Logger log() {
    return LOG;
  }

  @Override
  public void getState(OutputStream output) throws Exception {
    logTrace(() -> "CacheManager get state");
    try (ObjectOutputStream oos = new ObjectOutputStream(output)) {
      oos.writeInt(multiMaps.size());
      if (multiMaps.size() > 0) {
        oos.writeObject(multiMaps);
      }
      oos.writeInt(maps.size());
      if (maps.size() > 0) {
        oos.writeObject(maps);
      }
      oos.flush();
    }
  }

  @Override
  public void setState(InputStream input) throws Exception {
    logTrace(() -> "CacheManager set state");
    try (ObjectInputStream oos = new ObjectInputStream(input)) {
      if (oos.readInt() > 0) {
        Map<String, MultiMapImpl> m = (Map<String, MultiMapImpl>) oos.readObject();
        multiMaps.putAll(m);
      }
      if (oos.readInt() > 0) {
        Map<String, Map> m = (Map<String, Map>) oos.readObject();
        maps.putAll(m);
      }
    }
  }

  public void start() {
    try {
      channel.getState(null, 10000);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  public void stop() {
    dispatcher.stop();
  }
}
