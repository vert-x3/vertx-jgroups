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

package io.vertx.java.spi.cluster.impl.jgroups.domain;

import org.jgroups.util.Util;

import java.io.*;
import java.util.Iterator;

public class ImmutableChoosableSetImpl<T> implements ImmutableChoosableSet<T> {

  private T value;
  private ImmutableChoosableSet<T> next;

  private transient ImmutableChoosableSet<T> roundRobinState = this;

  public ImmutableChoosableSetImpl() {
  }

  private ImmutableChoosableSetImpl(T value, ImmutableChoosableSet<T> next) {
    this.value = value;
    this.next = next;
  }

  public ImmutableChoosableSetImpl(T value) {
    this(value, ImmutableChoosableSet.emptySet);
  }

  @Override
  public ImmutableChoosableSet<T> add(T value) {
    checkSanity(value);

    return new ImmutableChoosableSetImpl<>(value, this);
  }

  @Override
  public ImmutableChoosableSet<T> remove(T value) {
    checkSanity(value);

    if (value.equals(this.value)) {
      return next;
    }
    return next.remove(value).add(value);
  }

  @Override
  public T head() {
    return value;
  }

  @Override
  public ImmutableChoosableSet<T> tail() {
    return next;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }


  @Override
  public T choose() {
    if (this.roundRobinState.isEmpty()) {
      this.roundRobinState = this;
    }
    T value = (T) this.roundRobinState.head();
    this.roundRobinState = this.roundRobinState.tail();
    return value;
  }

  @Override
  public Iterator<T> iterator() {
    return new ImmutableIterator<T>(value, next.iterator());
  }

  private void checkSanity(T value) {
    if (value == null) {
      throw new IllegalArgumentException("Not supported null value.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ImmutableChoosableSetImpl)) {
      return false;
    }

    ImmutableChoosableSetImpl that = (ImmutableChoosableSetImpl) o;

    if ((value != null) && (that.value != null)) {
      return value.equals(that.value) && next.equals(that.next);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + next.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "[" + value + ", " + next + "]";
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      writeTo(out);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    try {
      readFrom(in);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void writeTo(DataOutput out) throws Exception {
    Util.writeObject(value, out);
    Util.writeObject(next, out);
  }

  @Override
  public void readFrom(DataInput in) throws Exception {
    value = (T) Util.readObject(in);
    next = (ImmutableChoosableSet<T>) Util.readObject(in);
  }
}
