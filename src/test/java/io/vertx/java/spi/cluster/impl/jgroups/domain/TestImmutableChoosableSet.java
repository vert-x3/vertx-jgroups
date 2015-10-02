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

import java.util.Iterator;

public class TestImmutableChoosableSet {

  @Test
  public void testOneElementSet() {
    String expected = "expected";
    ImmutableChoosableSet<String> set = new ImmutableChoosableSetImpl<>(expected);

    Assert.assertEquals(expected, set.choose());
    Assert.assertEquals(expected, set.choose());
  }

  @Test
  public void testTwoElementSet() {
    String expectedOne = "expectedOne";
    String expectedTwo = "expectedTwo";
    ImmutableChoosableSet<String> set = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo);

    Assert.assertEquals(expectedTwo, set.choose());
    Assert.assertEquals(expectedOne, set.choose());
    Assert.assertEquals(expectedTwo, set.choose());
    Assert.assertEquals(expectedOne, set.choose());
  }

  @Test
  public void testMoreElementSet() {
    String expectedOne = "expectedOne";
    String expectedTwo = "expectedTwo";
    String expectedThree = "expectedThree";
    String expectedFour = "expectedFour";
    String expectedFive = "expectedFive";

    ImmutableChoosableSet<String> value = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo);

    Assert.assertEquals(expectedTwo, value.choose());
    Assert.assertEquals(expectedOne, value.choose());

    value = value.add(expectedThree).add(expectedFour).add(expectedFive);

    Assert.assertEquals(expectedFive, value.choose());
    Assert.assertEquals(expectedFour, value.choose());
    Assert.assertEquals(expectedThree, value.choose());
    Assert.assertEquals(expectedTwo, value.choose());
    Assert.assertEquals(expectedOne, value.choose());
    Assert.assertEquals(expectedFive, value.choose());
    Assert.assertEquals(expectedFour, value.choose());
    Assert.assertEquals(expectedThree, value.choose());
    Assert.assertEquals(expectedTwo, value.choose());
    Assert.assertEquals(expectedOne, value.choose());
  }

  @Test
  public void testIteratorOneElementSet() {
    String expected = "expected";

    ImmutableChoosableSet<String> value = new ImmutableChoosableSetImpl<>(expected);

    Iterator<String> iterator = value.iterator();
    Assert.assertEquals(expected, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorTwoElementSet() {
    String expectedOne = "expectedOne";
    String expectedTwo = "expectedTwo";

    ImmutableChoosableSet<String> value = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo);

    Iterator<String> iterator = value.iterator();
    Assert.assertEquals(expectedTwo, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedOne, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorMoreElementSet() {
    String expectedOne = "expectedOne";
    String expectedTwo = "expectedTwo";
    String expectedThree = "expectedThree";
    String expectedFour = "expectedFour";
    String expectedFive = "expectedFive";

    ImmutableChoosableSet<String> value = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo);

    Iterator<String> iterator = value.iterator();
    Assert.assertEquals(expectedTwo, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedOne, iterator.next());
    Assert.assertFalse(iterator.hasNext());

    value = value.add(expectedThree).add(expectedFour).add(expectedFive);

    iterator = value.iterator();
    Assert.assertEquals(expectedFive, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedFour, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedThree, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedTwo, iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedOne, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorRemoveElement() {
    String expectedOne = "expectedOne";
    String expectedTwo = "expectedTwo";
    String expectedThree = "expectedThree";
    String expectedFour = "expectedFour";
    String expectedFive = "expectedFive";

    ImmutableChoosableSet<String> expected = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo).add(expectedThree).add(expectedFive);
    ImmutableChoosableSet<String> value = new ImmutableChoosableSetImpl<>(expectedOne).add(expectedTwo).add(expectedThree).add(expectedFour).add(expectedFive);

    value.remove(expectedFour);


    Assert.assertEquals(expected, value);
  }
}
