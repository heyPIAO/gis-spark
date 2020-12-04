package edu.zju.gis.hls.trajectory.analysis.index.partitioner;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import scala.Tuple2;

import java.util.List;

/**
 * 根据采样Sample构建索引得到分区规则
 * @author Hu
 * @date 2020/8/24
 **/
@Getter
@Setter
@ToString(callSuper = true)
public abstract class PreBuildSpatialPartitioner extends SpatialPartitioner {

  public PreBuildSpatialPartitioner(int partitionNum) {
    super(partitionNum);
  }

  public abstract <V extends Feature> void build(List<Tuple2<String,V>> samples);

}
