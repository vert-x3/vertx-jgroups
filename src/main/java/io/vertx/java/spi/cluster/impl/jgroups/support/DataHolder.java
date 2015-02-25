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

package io.vertx.java.spi.cluster.impl.jgroups.support;

import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DataHolder<T> implements Externalizable {

  private T data;

  public DataHolder() {
  }

  public static <T> DataHolder wrap(T object) {
    DataHolder dataHolder = new DataHolder();
    dataHolder.data = object;
    return dataHolder;
  }

  public T unwrap() {
    return data;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    boolean isClusterSerializable = ClusterSerializable.class.isInstance(data);
    out.writeBoolean(isClusterSerializable);
    if (isClusterSerializable) {
      out.writeUTF(data.getClass().getName());
      byte[] bytes = ((ClusterSerializable) data).writeToBuffer().getBytes();
      out.write(bytes.length);
      out.write(bytes);
    } else {
      out.writeObject(data);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    boolean isClusterSerializable = in.readBoolean();
    if (isClusterSerializable) {
      String className = in.readUTF();
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      try {
        data = (T) clazz.newInstance();
        byte[] bytes = new byte[in.read()];
        in.read(bytes);
        ((ClusterSerializable) data).readFromBuffer(Buffer.buffer(bytes));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new VertxException(e);
      }
    } else {
      data = (T) in.readObject();
    }
  }

}
