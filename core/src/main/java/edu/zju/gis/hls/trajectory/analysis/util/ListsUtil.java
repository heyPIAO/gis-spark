package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.index.hilbertcurve.Iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public final class ListsUtil {
  private ListsUtil() {
  }

  public static <E> ArrayList<E> newArrayList(E... elements) {
    PreconditionsUtils.checkNotNull(elements);
    int capacity = computeArrayListCapacity(elements.length);
    ArrayList<E> list = new ArrayList(capacity);
    Collections.addAll(list, elements);
    return list;
  }
  static int computeArrayListCapacity(int arraySize) {
    PreconditionsUtils.checkArgument(arraySize >= 0, "arraySize must be non-negative");
    return saturatedCast(5L + (long)arraySize + (long)(arraySize / 10));
  }

  static int saturatedCast(long value) {
    if (value > 2147483647L) {
      return 2147483647;
    } else {
      return value < -2147483648L ? -2147483648 : (int)value;
    }
  }

  public static <E> ArrayList<E> newArrayList() {
    return new ArrayList();
  }

  public static <E> ArrayList<E> newArrayList(Iterable<? extends E> elements) {
    PreconditionsUtils.checkNotNull(elements);
    return elements instanceof Collection ? new ArrayList((Collection)(elements)) : newArrayList(elements.iterator());
  }

  public static <E> ArrayList<E> newArrayList(Iterator<? extends E> elements) {
    ArrayList<E> list = newArrayList();
    Iterators.addAll(list, elements);
    return list;
  }
}
