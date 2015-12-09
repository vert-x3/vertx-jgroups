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

import java.io.Externalizable;

public interface ChoosableArrayList<T> extends Streamable, Externalizable, ChoosableIterable<T> {

  ChoosableArrayList<T> add(T value);

  ChoosableArrayList<T> remove(T value);

  T first();

  int size();

  ChoosableArrayList emptyChoosable = new EmptyChoosableArrayList<>();
}
