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

package io.vertx.spi.cluster.jgroups.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.spi.cluster.jgroups.impl.domain.MultiMap;
import io.vertx.spi.cluster.jgroups.impl.domain.SyncMapWrapper;
import io.vertx.spi.cluster.jgroups.impl.domain.async.AsyncMapWrapper;
import io.vertx.spi.cluster.jgroups.impl.domain.async.AsyncMultiMapWrapper;
import io.vertx.spi.cluster.jgroups.impl.services.*;
import io.vertx.spi.cluster.jgroups.impl.support.LambdaLogger;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.RpcDispatcher;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class CacheManager extends ReceiverAdapter implements LambdaLogger {

    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    private JChannel channel;
    private final RpcDispatcher dispatcher;

    private final RpcExecutorService executorService;
    private final RpcMultiMapService multiMapService;
    private final RpcMapService mapService;

    public CacheManager(Vertx vertx, JChannel channel) {
        this.channel = channel;

        this.multiMapService = new DefaultRpcMultiMapService();
        this.mapService = new DefaultRpcMapService();

        RpcServerObjDelegate server_obj = new RpcServerObjDelegate(mapService, multiMapService);
        // Don't want to loose the channel receiver.
        this.dispatcher = new RpcDispatcher(this.channel, this, channel.getReceiver(), server_obj);
        this.dispatcher.setMethodLookup(server_obj.getMethodLookup());

        this.executorService = new DefaultRpcExecutorService(vertx, dispatcher);
    }

    public <K, V> AsyncMultiMap<K, V> createAsyncMultiMap(String name) {
        logDebug(() -> String.format("method createAsyncMultiMap address[%s] name[%s]", channel.getAddressAsString(), name));
        MultiMap<K, V> map = multiMapService.<K, V>multiMapCreate(name);
        return new AsyncMultiMapWrapper<>(name, map, executorService);
    }

    public <K, V> AsyncMap<K, V> createAsyncMap(String name) {
        logDebug(() -> String.format("method createAsyncMap address[%s] name[%s]", channel.getAddressAsString(), name));
        Map<K, V> map = mapService.<K, V>mapCreate(name);
        return new AsyncMapWrapper<>(name, map, executorService);
    }

    public <K, V> Map<K, V> createSyncMap(String name) {
        logDebug(() -> String.format("method createSyncMap address[%s] name[%s]", channel.getAddressAsString(), name));
        Map<K, V> map = mapService.<K, V>mapCreate(name);
        return new SyncMapWrapper<>(name, map, executorService);
    }

    @Override
    public Logger log() {
        return LOG;
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        logTrace(() -> "CacheManager get state");
        multiMapService.writeTo(output);
        mapService.writeTo(output);
    }

    @Override
    public void setState(InputStream input) throws Exception {
        logTrace(() -> "CacheManager set state");
        multiMapService.readFrom(input);
        mapService.readFrom(input);
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
        executorService.stop();
    }
}
