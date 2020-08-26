package edu.zju.gis.hls.trajectory.analysis.index.partitioner;

/**
 * 根据采样Sample构建索引得到分区规则
 * @author Hu
 * @date 2020/8/24
 **/
public abstract class PreBuildDistributeSpatialPartitioner extends DistributeSpatialPartitioner {



  public PreBuildDistributeSpatialPartitioner(int partitionNum) {
    super(partitionNum);
  }

}
