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
    ChoosableArrayList<Long> expected = new ChoosableArrayListImpl<Long>().add(1l);
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
      Assert.assertTrue(ChoosableArrayList.class.isAssignableFrom(object.getClass()));

      ChoosableArrayList<Long> value = (ChoosableArrayList<Long>) object;
      Assert.assertEquals(expected.first(), value.first());
      // Should be empty set
      Assert.assertEquals(expected.size(), value.size());
    }
  }

  @Test
  public void testEmptyValue() throws Exception {
    ChoosableArrayList<Long> expected = ChoosableArrayList.emptyChoosable;
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
      Assert.assertTrue(ChoosableArrayList.class.isAssignableFrom(object.getClass()));

      ChoosableArrayList<Long> value = (ChoosableArrayList<Long>) object;
      // Should be empty set
      Assert.assertTrue(value.isEmpty());
    }
  }
}
