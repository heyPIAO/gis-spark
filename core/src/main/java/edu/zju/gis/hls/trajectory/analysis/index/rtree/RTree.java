package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2019/12/30
 * 目前 RTree 索引是用 Geotools 的 STRTree 实现的
 * TODO 有点纠结要不要将 RTree 直接extends Feature，方便 InnerRTreeIndexLayer 的 indexPartition，直接构建 RTreeLayer？，RTreeLayer的Geometry类型与Feature的Geometry类型一致，但是好像有点过于复杂？
 * TODO query时没有考虑CRS不一致的情况
 * **/
@Slf4j
@ToString(callSuper = true)
public class RTree implements Serializable {

  private STRtree si;

  @Getter
  private boolean isFinish;

  @Setter
  @Getter
  private CoordinateReferenceSystem crs;

  public RTree(int nodeCapacity, CoordinateReferenceSystem crs) {
    si = new STRtree(nodeCapacity);
    this.isFinish = false;
    this.crs = crs;
  }

  public void insert(List<Tuple2<String, Feature>> features) {
    for (Tuple2<String, Feature> feature: features) {
      this.insert(feature._2.getGeometry().getEnvelopeInternal(), feature._1, feature._2);
    }
  }

  public void insert(Envelope e, String index, Feature o)  {
    if (!this.isFinish) {
      this.si.insert(e, new Tuple2<String, Feature>(index, o));
    } else {
      log.warn("RTree has already built");
    }
  }

  public List<Tuple2<String, Feature>> query(Envelope e) {
    List<Tuple2<String, Feature>> result = (List<Tuple2<String, Feature>>) this.si.query(e);
    if (result.size() == 0) {
      result.add(generateDaumNode());
    }
    return result;
  }

  /**
   * Hint: DaumNode 的 Geometry 为 CRS 的 Envelope
   * @param
   * @return
   */
  private Tuple2<String, Feature>  generateDaumNode() {
    return new Tuple2<String, Feature>(Term.DAUM_KEY, new Polygon(GeometryUtil.envelopeToPolygon(CrsUtils.getCrsEnvelope(this.crs))));
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

  public List<Tuple2<String, Feature>> query(Geometry g) {
    List<Tuple2<String, Feature>> e = this.query(g.getEnvelopeInternal());
    if (e.size() == 1 && e.get(0)._1.equals(Term.DAUM_KEY)) return e;
    return e.stream()
      .filter(v -> v._2.getGeometry().intersects(g))
      .collect(Collectors.toList());
  }

  /**
   * 用一个HashMap辅助key查询
   * @param key
   * @return
   */
  public Tuple2<String, Feature> query(String key) {
    List<Tuple2<String, Feature>> q = (List<Tuple2<String, Feature>>)this.si.itemsTree();
    q = q.stream().filter(x->x._1.equals(key)).collect(Collectors.toList());
    if (q.size() == 0) q.add(this.generateDaumNode());
    return q.get(0);
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
      && Objects.equals(si.size(), rTree.si.size())
      && crs.toWKT().equals(rTree.crs.toWKT());
  }

}
