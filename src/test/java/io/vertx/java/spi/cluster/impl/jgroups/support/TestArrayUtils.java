package io.vertx.java.spi.cluster.impl.jgroups.support;

import org.jgroups.util.UUID;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestArrayUtils {

  @Test
  public void testFilterDuplicates() {
    String node1 = "node1";
    String node2 = "node2";
    String node3 = "node3";
    String node4 = "node4";
    String node5 = "node5";
    String node6 = "node6";

    String[] nodes = new String[]{node1, node2, node4, node5, node3, node4, node5, node6};

    Arrays.sort(nodes);

    String[] filtered = ArrayUtils.filterDuplicatesInSortedArray(nodes);

    Assert.assertEquals(6, filtered.length);
    Assert.assertEquals(node1, filtered[0]);
    Assert.assertEquals(node2, filtered[1]);
    Assert.assertEquals(node3, filtered[2]);
    Assert.assertEquals(node4, filtered[3]);
    Assert.assertEquals(node5, filtered[4]);
    Assert.assertEquals(node6, filtered[5]);
  }

  @Test
  public void testFilterDuplicatesNoDuplicates() {
    String node1 = "node1";
    String node2 = "node2";
    String node3 = "node3";
    String node4 = "node4";

    String[] nodes = new String[]{node1, node2, node3, node4};

    Arrays.sort(nodes);

    String[] filtered = ArrayUtils.filterDuplicatesInSortedArray(nodes);

    Assert.assertEquals(4, filtered.length);
    Assert.assertEquals(node1, filtered[0]);
    Assert.assertEquals(node2, filtered[1]);
    Assert.assertEquals(node3, filtered[2]);
    Assert.assertEquals(node4, filtered[3]);
  }
  @Test
  public void testFilterDuplicatesOneElement() {
    String node1 = "node1";

    String[] nodes = new String[]{node1};

    Arrays.sort(nodes);

    String[] filtered = ArrayUtils.filterDuplicatesInSortedArray(nodes);

    Assert.assertEquals(1, filtered.length);
    Assert.assertEquals(node1, filtered[0]);
  }

  @Test
  public void testFilterDuplicatesEmptyArray() {
    String[] nodes = new String[]{};

    Arrays.sort(nodes);

    String[] filtered = ArrayUtils.filterDuplicatesInSortedArray(nodes);

    Assert.assertEquals(0, filtered.length);
  }

  @Test
  public void testArraySameSize() {
    UUID node1 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node2 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node3 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node4 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node5 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node6 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID[] left = new UUID[]{node1, node3, node4, node5};
    UUID[] right = new UUID[]{node1, node2, node4, node6};

    Arrays.sort(left);
    Arrays.sort(right);

    List<ComparedValue<UUID>> comparedValues = ArrayUtils.compareSorted(left, right);

    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node1, ComparedType.EQUAL)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node2, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node3, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node4, ComparedType.EQUAL)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node5, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node6, ComparedType.RIGHT)));
  }

  @Test
  public void testArrayLeftSideEmpty() {
    UUID node1 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node2 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node4 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node6 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID[] left = new UUID[]{};
    UUID[] right = new UUID[]{node1, node2, node4, node6};

    Arrays.sort(left);
    Arrays.sort(right);

    List<ComparedValue<UUID>> comparedValues = ArrayUtils.compareSorted(left, right);

    System.out.println(comparedValues);

    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node1, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node2, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node4, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node6, ComparedType.RIGHT)));
  }

  @Test
  public void testArrayRightSideEmpty() {
    UUID node1 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node2 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node3 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node4 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node5 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node6 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID[] left = new UUID[]{node1, node2, node4, node6};
    UUID[] right = new UUID[]{};

    Arrays.sort(left);
    Arrays.sort(right);

    List<ComparedValue<UUID>> comparedValues = ArrayUtils.compareSorted(left, right);

    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node1, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node2, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node4, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node6, ComparedType.LEFT)));
  }

  @Test
  public void testArrayTotallyDifferent() {
    UUID node1 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node2 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node3 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node4 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node5 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID node6 = new UUID((long) (Math.random() * Long.MAX_VALUE), (long) (Math.random() * Long.MAX_VALUE));
    UUID[] left = new UUID[]{node1, node2, node4};
    UUID[] right = new UUID[]{node3, node5, node6};

    Arrays.sort(left);
    Arrays.sort(right);

    List<ComparedValue<UUID>> comparedValues = ArrayUtils.compareSorted(left, right);

    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node1, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node2, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node3, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node4, ComparedType.LEFT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node5, ComparedType.RIGHT)));
    Assert.assertTrue(comparedValues.contains(new ComparedValue<>(node6, ComparedType.RIGHT)));
  }

}
