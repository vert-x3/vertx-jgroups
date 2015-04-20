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

import java.util.Collections;
import java.util.Iterator;

public class ImmutableIterator<T> implements Iterator<T> {

  private T value;
  private Iterator<T> nextIterator = Collections.emptyIterator();

  private boolean onNextIterator = false;

  public ImmutableIterator(T value) {
    this.value = value;
  }

  public ImmutableIterator(T value, Iterator<T> nextIterator) {
    this.value = value;
    this.nextIterator = nextIterator;
  }

  @Override
  public boolean hasNext() {
    return (!onNextIterator) || nextIterator.hasNext();
  }

  @Override
  public T next() {
    if (!onNextIterator) {
      onNextIterator = true;
      return value;
    }
    return nextIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
