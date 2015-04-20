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

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestMultiMapImpl {

  @Test
  public void testExternalizableOnEmptyMap() throws IOException, ClassNotFoundException {
    MultiMapImpl<Long, String> expected = new MultiMapImpl<Long, String>();

    byte[] buffer;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ObjectOutputStream stream = new ObjectOutputStream(out);) {
      expected.writeExternal(stream);
      stream.flush();
      buffer = out.toByteArray();
    }

    MultiMapImpl object = new MultiMapImpl();
    try (ByteArrayInputStream input = new ByteArrayInputStream(buffer);
         ObjectInputStream stream = new ObjectInputStream(input);) {
      object.readExternal(stream);
    }
  }

  @Test
  public void testExternalizable() throws IOException, ClassNotFoundException {
    MultiMapImpl<Long, String> expected = new MultiMapImpl<Long, String>();
    List<String> as = new ArrayList<>(Arrays.asList("a1", "a2", "a3", "a4"));
    List<String> bs = new ArrayList<>(Arrays.asList("b1"));
    List<String> cs = new ArrayList<>(Arrays.asList("c1", "c2"));

    expected.add(1L, "a1");
    expected.add(1L, "a2");
    expected.add(1L, "a3");
    expected.add(1L, "a4");
    expected.add(2L, "b1");
    expected.add(3L, "c1");
    expected.add(3L, "c2");

    byte[] buffer;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ObjectOutputStream stream = new ObjectOutputStream(out);) {
      expected.writeExternal(stream);
      stream.flush();
      buffer = out.toByteArray();
    }

    MultiMapImpl object = new MultiMapImpl();
    try (ByteArrayInputStream input = new ByteArrayInputStream(buffer);
         ObjectInputStream stream = new ObjectInputStream(input);) {
      object.readExternal(stream);
    }

    ImmutableChoosableSet values = object.get(1L);
    Assert.assertNotNull(values);
    while (!values.isEmpty()) {
      Assert.assertTrue("Do not contain " + values.head(), as.contains(values.head()));
      as.remove(values.head());
      values = values.tail();
    }
    values = object.get(2L);
    Assert.assertNotNull(values);
    while (!values.isEmpty()) {
      Assert.assertTrue("Do not contain " + values.head(), bs.contains(values.head()));
      bs.remove(values.head());
      values = values.tail();
    }
    values = object.get(3L);
    Assert.assertNotNull(values);
    while (!values.isEmpty()) {
      Assert.assertTrue("Do not contain " + values.head(), cs.contains(values.head()));
      cs.remove(values.head());
      values = values.tail();
    }
  }
}
