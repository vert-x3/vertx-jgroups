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

import io.vertx.core.spi.cluster.ChoosableIterable;
import org.jgroups.util.Streamable;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;

public interface ImmutableChoosableSet<T> extends Streamable, Externalizable, ChoosableIterable<T> {

  ImmutableChoosableSet<T> add(T value);

  ImmutableChoosableSet<T> remove(T value);

  T head();

  ImmutableChoosableSet<T> tail();

  public static final ImmutableChoosableSet emptySet = new EmptyImmutableChoosableSet();

  public class EmptyImmutableChoosableSet implements ImmutableChoosableSet {

    public EmptyImmutableChoosableSet() {
    }

    @Override
    public ImmutableChoosableSet add(Object value) {
      return new ImmutableChoosableSetImpl(value);
    }

    @Override
    public ImmutableChoosableSet remove(Object value) {
      return this;
    }

    @Override
    public Object head() {
      return null;
    }

    @Override
    public ImmutableChoosableSet tail() {
      return this;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Object choose() {
      return null;
    }

    @Override
    public Iterator iterator() {
      return Collections.emptyIterator();
    }


    @Override
    public String toString() {
      return "[]";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
    }
  }
}
