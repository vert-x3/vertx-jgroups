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
import java.util.Iterator;
import java.util.List;

public class TestMultiMapImpl {

  @Test
  public void testExternalizableOnEmptyMap() throws IOException, ClassNotFoundException {
    MultiMapImpl<Long, String> expected = new MultiMapImpl<Long, String>("__cache");

    byte[] buffer;
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ObjectOutputStream stream = new ObjectOutputStream(out);) {
      expected.writeExternal(stream);
      stream.flush();
      buffer = out.toByteArray();
    }

    MultiMapImpl object = new MultiMapImpl("__cache");
    try (ByteArrayInputStream input = new ByteArrayInputStream(buffer);
         ObjectInputStream stream = new ObjectInputStream(input);) {
      object.readExternal(stream);
    }
  }

  @Test
  public void testRemoveAll() throws IOException, ClassNotFoundException {
    String EXPECTED_VALUE1 = "value1";
    String EXPECTED_VALUE2 = "value2";
    String EXPECTED_VALUE3 = "value3";

    MultiMapImpl<Long, String> multiMap = new MultiMapImpl<Long, String>("__cache");

    multiMap.add(1L, EXPECTED_VALUE1);
    multiMap.add(2L, EXPECTED_VALUE1);
    multiMap.add(2L, EXPECTED_VALUE2);
    multiMap.add(2L, EXPECTED_VALUE3);
    multiMap.add(3L, EXPECTED_VALUE2);
    multiMap.add(3L, EXPECTED_VALUE3);

    Assert.assertNotNull(multiMap.get(1L));
    Assert.assertEquals(EXPECTED_VALUE1, multiMap.get(1L).first());
    Assert.assertNotNull(multiMap.get(2L));
    Assert.assertEquals(3, multiMap.get(2L).size());
    Assert.assertEquals(EXPECTED_VALUE1, multiMap.get(2L).choose());
    Assert.assertEquals(EXPECTED_VALUE2, multiMap.get(2L).choose());
    Assert.assertEquals(EXPECTED_VALUE3, multiMap.get(2L).choose());
    Assert.assertNotNull(multiMap.get(3L));
    Assert.assertEquals(2, multiMap.get(3L).size());
    Assert.assertEquals(EXPECTED_VALUE2, multiMap.get(3L).choose());
    Assert.assertEquals(EXPECTED_VALUE3, multiMap.get(3L).choose());

    multiMap.removeAll(EXPECTED_VALUE2);

    Assert.assertNotNull(multiMap.get(1L));
    Assert.assertEquals(EXPECTED_VALUE1, multiMap.get(1L).first());
    Assert.assertNotNull(multiMap.get(2L));
    Assert.assertEquals(2, multiMap.get(2L).size());
    Assert.assertEquals(EXPECTED_VALUE1, multiMap.get(2L).choose());
    Assert.assertEquals(EXPECTED_VALUE3, multiMap.get(2L).choose());
    Assert.assertNotNull(multiMap.get(3L));
    Assert.assertEquals(1, multiMap.get(3L).size());
    Assert.assertEquals(EXPECTED_VALUE3, multiMap.get(3L).choose());
  }

  @Test
  public void testExternalizable() throws IOException, ClassNotFoundException {
    MultiMapImpl<Long, String> expected = new MultiMapImpl<Long, String>("__cache");
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

    MultiMapImpl object = new MultiMapImpl("__cache");
    try (ByteArrayInputStream input = new ByteArrayInputStream(buffer);
         ObjectInputStream stream = new ObjectInputStream(input);) {
      object.readExternal(stream);
    }

    ChoosableArrayList values = object.get(1L);
    Assert.assertNotNull(values);
    Iterator iterator = values.iterator();
    while(iterator.hasNext()) {
      Object next = iterator.next();
      Assert.assertTrue("Do not contain " + next, as.contains(next));
      as.remove(next);
    }
    Assert.assertEquals(0, as.size());

    values = object.get(2L);
    Assert.assertNotNull(values);
    iterator = values.iterator();
    while(iterator.hasNext()) {
      Object next = iterator.next();
      Assert.assertTrue("Do not contain " + next, bs.contains(next));
      bs.remove(next);
    }
    Assert.assertEquals(0, bs.size());

    values = object.get(3L);
    Assert.assertNotNull(values);
    iterator = values.iterator();
    while(iterator.hasNext()) {
      Object next = iterator.next();
      Assert.assertTrue("Do not contain " + next, cs.contains(next));
      cs.remove(next);
    }
    Assert.assertEquals(0, cs.size());
  }
}
