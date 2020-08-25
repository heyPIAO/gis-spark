package edu.zju.gis.hls.trajectory.analysis.index.partitioner;


import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

import java.util.List;

/**
 * 根据空间划分规则实时计算得到 geometry 对应分区的 Distribute Spatial Partitioner
 * @author Hu
 * @date 2020/8/24
 **/
public abstract class SpaceSplitDistributeSpatialPartitioner extends DistributeSpatialPartioner {

  /**
   * 根据分区结果收集分区元数据信息
   * 对于 SpaceSplitSpatialPartitioner 方法，需要在分区后才能收集数据分布信息，keyRanges 在这时才能初始化
   * 对于 PreBuildSpatialParitioner方法，数据分布信息在分区前即可获得，keyRanges 在分区发生前即可初始化
   * TODO 这个方法有点蠢，应该在分区shuffle的时候就将分区信息记录在共享内存中
   */
  public void collectPartitionMeta(Layer layer) {
    List<String> keys = layer.distinctKeys();
    keys.forEach(key -> keyRanges.put(key, this.getKeyRangeFeature(key)));
  }


}
