package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Serializable;

/**
 * @author Hu
 * @date 2020/12/10
 * 二维指定层级横纵相同网格划分
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class UniformGridIndex implements DistributeSpatialIndex, Serializable {

  private UniformGridConfig c;

  public UniformGridIndex() {
    this(new UniformGridConfig());
  }

  public UniformGridIndex(UniformGridConfig c) {
    this.c = c;
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer) {
    return this.index(layer, true);
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges) {
    return this.index(layer, withKeyRanges, -1);
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges, int numPartitions) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    UniformGridPartitioner partitioner = new UniformGridPartitioner(numPartitions);
    partitioner.setCrs(crs);
    partitioner.setConf(c);
    UniformGridIndexLayer<L> result = new UniformGridIndexLayer<L>(partitioner);
    L l = (L) layer.flatMapToLayer(partitioner);
    l.makeSureCached();
    if (numPartitions == -1) partitioner.setPartitionNum(l.distinctKeys().size());
    L klayer = (L)l.partitionByToLayer(partitioner);
    l.unpersist();
    if (withKeyRanges) {
      partitioner.collectPartitionMeta(klayer);
    }
    result.setLayer(klayer);
    result.setPartitioner(partitioner);
    return (T) result;
  }

}
