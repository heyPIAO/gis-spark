package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.PreBuildSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/8/26
 * 基于 R 树的分区器
 **/
@Getter
@ToString(callSuper = true)
@Slf4j
public class RTreePartitioner extends PreBuildSpatialPartitioner {

  private RTree rTree;
  private RTreeIndexConfig conf;
  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;
  private Envelope extent = CrsUtils.getCrsEnvelope(this.crs);

  public RTreePartitioner(int partitionNum) {
    super(partitionNum);
  }

  public void setCrs(CoordinateReferenceSystem crs) {
    this.crs = crs;
    this.extent = CrsUtils.getCrsEnvelope(this.crs);
  }

  public void setConf(RTreeIndexConfig conf) {
    this.conf = conf;
    this.rTree = new RTree(this.conf.getSampleSize()/partitionNum, this.crs);
    this.isClip = conf.isClip();
  }

  /**
   * 重写根据空间范围查询分区方法
   * @param geometry
   * @return
   */
  @Override
  public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
    List<Tuple2<String, Feature>> qs = this.rTree.query(geometry);
    List<KeyRangeFeature> result = new ArrayList<>();
    for (Tuple2<String, Feature> q: qs) {
      result.add(this.generateKeyRangeFeature(q._1, (Polygon)q._2.getGeometry().getEnvelope()));
    }
    return result;
  }

  /**
   * 重写根据key查询对应分区空间范围信息的方法
   * @param key
   * @return
   */
  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (key.equals(RTree.DAUM_KEY)) {
      return new KeyRangeFeature(key,
        GeometryUtil.envelopeToPolygon(CrsUtils.getCrsEnvelope(this.crs)),
        getPartition(key));
    }
    return new KeyRangeFeature(key,
      (Polygon)(this.rTree.query(key))._2.getGeometry(),
      getPartition(key));
  }

  @Override
  public <V extends Feature> void build(List<Tuple2<String, V>> samples) {
    this.rTree.insert(samples.stream().map(x->(Tuple2<String, Feature>)x).collect(Collectors.toList()));
    this.rTree.finish();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RTreePartitioner that = (RTreePartitioner) o;
    return Objects.equals(rTree, that.rTree);
  }

}
