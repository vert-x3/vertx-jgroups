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

package io.vertx.java.spi.cluster.impl.jgroups.services;

import org.jgroups.blocks.MethodCall;

public interface MethodCallInterface {

  public interface OneParameter extends MethodCallInterface {
    MethodCall method(String name);
  }

  public interface TwoParameters extends MethodCallInterface {
    MethodCall method(String name, Object p1);
  }

  public interface ThreeParameters extends MethodCallInterface {
    MethodCall method(String name, Object p1, Object p2);
  }

  public interface FourParameters extends MethodCallInterface {
    MethodCall method(String name, Object p1, Object p2, Object p3);
  }
}
