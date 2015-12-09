package io.vertx.java.spi.cluster.impl.jgroups.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayUtils {

  public static <T extends Comparable> T[] copySortAndFilter(T[] values) {
    T[] copyOf = Arrays.copyOf(values, values.length);
    Arrays.sort(copyOf);
    return ArrayUtils.filterDuplicatesInSortedArray(copyOf);
  }

  public static <T extends Comparable> T[] filterDuplicatesInSortedArray(T[] values) {
    if (values.length > 0) {
      int last = 0;
      for (int i = 1; i < values.length; i++) {
        if ((values[last] != null) && (values[i] != null) && (values[last].compareTo(values[i]) != 0)) {
          last += 1;
          values[last] = values[i];
        }
      }
      int newLength = last + 1;
      if (newLength < values.length) {
        return Arrays.copyOf(values, newLength);
      } else {
        return values;
      }
    } else {
      return values;
    }
  }

  public static <T extends Comparable> List<ComparedValue<T>> compareSorted(T[] leftArray, T[] rightArray) {
    List<ComparedValue<T>> comparedValues = new ArrayList<>(leftArray.length + rightArray.length);
    int leftIndex = 0;
    int rightIndex = 0;

    for (; (leftIndex < leftArray.length) || (rightIndex < rightArray.length); ) {
      T left = leftIndex < leftArray.length ? leftArray[leftIndex] : null;
      T right = rightIndex < rightArray.length ? rightArray[rightIndex] : null;

      int choose = compare(left, right);
      if (choose == 0) {
        leftIndex++;
        rightIndex++;
        comparedValues.add(new ComparedValue<>(left, ComparedType.EQUAL));
      } else {
        if (choose < 0) {
          leftIndex++;
          comparedValues.add(new ComparedValue<>(left, ComparedType.LEFT));
        } else {
          rightIndex++;
          comparedValues.add(new ComparedValue<>(right, ComparedType.RIGHT));
        }
      }
    }
    return comparedValues;
  }

  private static <T extends Comparable> int compare(T a, T b) {
    if (a == b) {
      return 0;
    }
    if (a == null || b == null) {
      return a == null ? 1 : -1;
    }
    return a.compareTo(b);
  }
}
