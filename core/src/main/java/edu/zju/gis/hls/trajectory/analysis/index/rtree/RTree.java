package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2019/12/30
 * 目前 RTree 索引是用 Geotools 的 STRTree 实现的
 * TODO 有点纠结要不要将 RTree 直接extends Feature，方便 InnerRTreeIndexLayer 的 indexPartition，直接构建 RTreeLayer？，RTreeLayer的Geometry类型与Feature的Geometry类型一致，但是好像有点过于复杂？
 **/
@Slf4j
public class RTree {

  private STRtree si;

  @Getter
  private boolean isFinish;

  public RTree(int nodeCapacity) {
    si = new STRtree(nodeCapacity);
    this.isFinish = false;
  }

  public <V extends Feature> void insert(List<Tuple2<String, V>> features) {
    for (Tuple2<String, V> feature: features) {
      this.insert(feature._2.getGeometry().getEnvelopeInternal(), feature._1, feature._2);
    }
  }

  public <V extends Feature> void insert(Envelope e, String index, V o)  {
    if (!this.isFinish) {
      this.si.insert(e, new Tuple2<String, V>(index, o));
    } else {
      log.warn("RTree has already built");
    }
  }

  public <V extends Feature> List<Tuple2<String, V>> query(Envelope e) {
    return (List<Tuple2<String, V>>) this.si.query(e);
  }

  /**
   * TODO 待测，不确定分布式环境下是否可用
   * @param e
   * @param o
   * @param <V>
   * @return
   */
  public <V extends Feature> boolean remove(Envelope e, Tuple2<String, V> o) {
    if (!this.isFinish) {
      return this.si.remove(e, o);
    }
    log.warn("RTree has already built");
    return false;
  }

  public <V extends Feature> List<Tuple2<String, V>> query(Geometry g) {
    List<Tuple2<String, V>> e = this.query(g.getEnvelopeInternal());
    return e.stream()
      .filter(v -> v._2.getGeometry().intersects(g))
      .collect(Collectors.toList());
  }

  public <V extends Feature> Tuple2<String, V> query(String key) {
    List<Tuple2<String, V>> r = (List<Tuple2<String, V>>)this.si.itemsTree().stream().filter(x-> {
      Tuple2<String, V> v = (Tuple2<String, V>) x;
      return v._1.equals(key);
    }).collect(Collectors.toList());

    return r.get(0);
  }

  public void finish() {
    this.si.build();
    this.isFinish = true;
  }

  /**
   * TODO Not Precise
   * 不精准的equal，仅判断envelope的size，而不判断envelope的具体范围
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RTree rTree = (RTree) o;
    return Objects.equals(si.depth(), rTree.si.depth())
      && Objects.equals(si.size(), rTree.si.size());
  }

}
