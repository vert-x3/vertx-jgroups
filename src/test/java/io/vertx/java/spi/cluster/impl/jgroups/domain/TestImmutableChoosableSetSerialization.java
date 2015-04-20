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
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class TestImmutableChoosableSetSerialization {

  @Test
  public void testOneValue() throws Exception {
    ImmutableChoosableSet<Long> expected = new ImmutableChoosableSetImpl<>(1l);
    byte[] buffer;

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         DataOutputStream outputStream = new DataOutputStream(bos)) {
      Util.writeObject(expected, outputStream);
      outputStream.flush();
      buffer = bos.toByteArray();
    }

    try (
        InputStream bis = new ByteArrayInputStream(buffer);
        DataInputStream inputStream = new DataInputStream(bis)) {
      Object object = Util.readObject(inputStream);

      Assert.assertNotNull(object);
      Assert.assertTrue(ImmutableChoosableSet.class.isAssignableFrom(object.getClass()));

      ImmutableChoosableSet<Long> value = (ImmutableChoosableSet<Long>) object;
      Assert.assertEquals(expected.head(), value.head());
      // Should be empty set
      Assert.assertEquals(expected.tail().isEmpty(), value.tail().isEmpty());
    }
  }

  @Test
  public void testEmptyValue() throws Exception {
    ImmutableChoosableSet<Long> expected = ImmutableChoosableSet.emptySet;
    byte[] buffer;

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         DataOutputStream outputStream = new DataOutputStream(bos)) {
      Util.writeObject(expected, outputStream);
      outputStream.flush();
      buffer = bos.toByteArray();
    }

    try (
        InputStream bis = new ByteArrayInputStream(buffer);
        DataInputStream inputStream = new DataInputStream(bis)) {
      Object object = Util.readObject(inputStream);

      Assert.assertNotNull(object);
      Assert.assertTrue(ImmutableChoosableSet.class.isAssignableFrom(object.getClass()));

      ImmutableChoosableSet<Long> value = (ImmutableChoosableSet<Long>) object;
      // Should be empty set
      Assert.assertTrue(value.isEmpty());
    }
  }
}
