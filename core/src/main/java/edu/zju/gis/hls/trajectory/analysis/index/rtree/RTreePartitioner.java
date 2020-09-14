package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.PreBuildDistributeSpatialPartitioner;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Objects;

/**
 * @author Hu
 * @date 2020/8/26
 * 基于 R 树的分区器
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class RTreePartitioner extends PreBuildDistributeSpatialPartitioner {

  private RTree rTree;

  public RTreePartitioner(int partitionNum) {
    super(partitionNum);
    this.rTree = new RTree();
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    return null;
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RTreePartitioner that = (RTreePartitioner) o;
    return Objects.equals(rTree, that.rTree);
  }

}
