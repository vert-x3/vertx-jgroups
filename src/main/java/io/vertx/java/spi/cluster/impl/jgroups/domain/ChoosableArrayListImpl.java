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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ChoosableArrayListImpl<T> implements ChoosableArrayList<T> {

  private volatile List<T> values = new CopyOnWriteArrayList<>();
  private volatile int roundRobinIndex = 0;

  public ChoosableArrayListImpl() {
  }

  @Override
  public ChoosableArrayList<T> add(T value) {
    values.add(value);
    return this;
  }

  @Override
  public ChoosableArrayList<T> remove(T value) {
    values.remove(value);
    return this;
  }

  @Override
  public T first() {
    return values.get(0);
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public boolean isEmpty() {
    return values.isEmpty();
  }

  @Override
  public T choose() {
    if (roundRobinIndex >= values.size()) {
      roundRobinIndex = 0;
    }
    return values.get(roundRobinIndex++);
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
  public void readExternal(ObjectInput in) throws IOException {
    try {
      readFrom(in);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void writeTo(DataOutput out) throws Exception {
    out.writeInt(values.size());
    for (T value : values) {
      Util.writeObject(value, out);
    }
  }

  @Override
  public void readFrom(DataInput in) throws Exception {
    int size = in.readInt();
    T[] buffer = (T[]) new Object[size];
    for (int i = 0; i < size; i++) {
      buffer[i] = (T) Util.readObject(in);
    }
    values = new CopyOnWriteArrayList<>(buffer);
  }

  @Override
  public Iterator<T> iterator() {
    return values.iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChoosableArrayListImpl)) {
      return false;
    }

    ChoosableArrayListImpl that = (ChoosableArrayListImpl) o;
    if (this.size() != that.size()) {
      return false;
    }
    for (T value : values) {
      if (!that.values.contains(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = values != null ? values.hashCode() : 0;
    result = 31 * result + roundRobinIndex;
    return result;
  }

  @Override
  public String toString() {
    String valuesToString = values.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", "));
    return "[" + valuesToString + "]";
  }
  /*
  private T value;
  private ChoosableArrayList<T> next;

  private transient ChoosableArrayList<T> roundRobinState = this;

  public ChoosableArrayListImpl() {
  }

  private ChoosableArrayListImpl(T value, ChoosableArrayList<T> next) {
    this.value = value;
    this.next = next;
  }

  public ChoosableArrayListImpl(T value) {
    this(value, ChoosableArrayList.emptySet);
  }

  @Override
  public ChoosableArrayList<T> add(T value) {
    checkSanity(value);

    return new ChoosableArrayListImpl<>(value, this);
  }

  @Override
  public ChoosableArrayList<T> remove(T value) {
    checkSanity(value);

    if (this.value.equals(value)) {
      this.value = this.next.head();
      this.next = this.next.tail();
      return this;
    } else {
      this.next = next.remove(value);
      return this;
    }
  }

  @Override
  public T head() {
    return value;
  }

  @Override
  public ChoosableArrayList<T> tail() {
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
    if (!(o instanceof ChoosableArrayListImpl)) {
      return false;
    }

    ChoosableArrayListImpl that = (ChoosableArrayListImpl) o;

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
    next = (ChoosableArrayList<T>) Util.readObject(in);
  }
*/
}
