package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2019/12/30
 * 目前 RTree 索引是用 geotools 的 STRTree 实现的
 * TODO 有点纠结要不要将RTree直接extends Feature，方便 InnerRTreeIndexLayer 的 indexPartition，直接构建 RTreeLayer？，RTreeLayer的Geometry类型与Feature的Geometry类型一致，但是好像有点过于复杂？
 **/
public class RTree {

  private STRtree si;

  @Getter
  private List<Envelope> grids;


  public RTree() {
    this.grids = new ArrayList<>();
    si = new STRtree();
  }

  public <V extends Feature> void insert(Envelope e, V o)  {
    this.si.insert(e, o);
  }

  public <V extends Feature> List<V> query(Envelope e) {
    return (List<V>) this.si.query(e);
  }

  public <V extends Feature> boolean remove(Envelope e, V o) {
    return this.si.remove(e, o);
  }

  public <V extends Feature> List<V> query(Geometry g) {
    List<V> e = this.query(g.getEnvelopeInternal());
    return e.stream().filter(new Predicate<V>() {
      @Override
      public boolean test(V v) {
        Feature f = (Feature) v;
        return f.getGeometry().intersects(g);
      }
    }).collect(Collectors.toList());
  }

  public void finish() {
    this.si.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RTree rTree = (RTree) o;
    return Objects.equals(si.depth(), rTree.si.depth())
      && Objects.equals(si.size(), rTree.si.size())
      && Objects.equals(grids.size(), rTree.grids.size());
  }
}
