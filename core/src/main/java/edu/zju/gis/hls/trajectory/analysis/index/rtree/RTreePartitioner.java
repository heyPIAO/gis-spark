package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.PreBuildDistributeSpatialPartitioner;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Hu
 * @date 2020/8/26
 * 基于 R 树的分区器
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class RTreePartitioner extends PreBuildDistributeSpatialPartitioner {

  public RTreePartitioner(int partitionNum) {
    super(partitionNum);
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    return null;
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    return null;
  }

}
