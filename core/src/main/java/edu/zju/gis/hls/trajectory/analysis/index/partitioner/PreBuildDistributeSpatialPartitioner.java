package edu.zju.gis.hls.trajectory.analysis.index.partitioner;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 根据采样Sample构建索引得到分区规则
 * @author Hu
 * @date 2020/8/24
 **/
@Getter
@Setter
@ToString(callSuper = true)
public abstract class PreBuildDistributeSpatialPartitioner extends DistributeSpatialPartitioner {

  public PreBuildDistributeSpatialPartitioner(int partitionNum) {
    super(partitionNum);
  }

}
