package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.PreBuildSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

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

  public static String DAUM_NODE_KEY = "DAUM";

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
    this.rTree = new RTree(this.conf.getSampleSize()/partitionNum);
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    return this.rTree.query(geometry).stream().
      map(x->x._1).collect(Collectors.toList());
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (key.equals(DAUM_NODE_KEY)) {
      log.warn("DAUM node key");
      return null;
    }
    return new KeyRangeFeature(key,
      (Polygon)(this.rTree.query(key))._2.getGeometry(),
      getPartition(key));
  }

  @Override
  public <V extends Feature> void build(List<Tuple2<String, V>> samples) {
    this.rTree.insert(samples);
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
