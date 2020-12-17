//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package edu.zju.gis.hls.trajectory.analysis.index.hilbertcurve;

import edu.zju.gis.hls.trajectory.analysis.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;

public final class Iterators {
  private Iterators() {
  }

  public static <T> boolean addAll(Collection<T> addTo, Iterator<? extends T> iterator) {
    Preconditions.checkNotNull(addTo);
    Preconditions.checkNotNull(iterator);

    boolean wasModified;
    for(wasModified = false; iterator.hasNext(); wasModified |= addTo.add(iterator.next())) {
      ;
    }

    return wasModified;
  }
}
