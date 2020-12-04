package edu.zju.gis.hls.trajectory.analysis.index.equalGrid;

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
 * @date 2020/12/3
 * 某一维度的单向固定宽度网格划分
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class EqualGridIndex implements DistributeSpatialIndex, Serializable {

  private EqualGridIndexConfig c;

  public EqualGridIndex() {
    this(new EqualGridIndexConfig());
  }

  public EqualGridIndex(EqualGridIndexConfig c) {
    this.c = c;
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer) {
    return this.index(layer, true);
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges) {
    return this.index(layer, withKeyRanges, c.getNum());
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges, int numPartitions) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    EqualGridPartitioner partitioner = new EqualGridPartitioner(numPartitions);
    partitioner.setConf(c);
    partitioner.setCrs(crs);
    EqualGridIndexLayer<L> result = new EqualGridIndexLayer<L>(partitioner);
    L klayer = (L) layer.flatMapToLayer(partitioner).partitionByToLayer(partitioner);
    if (withKeyRanges) {
      partitioner.collectPartitionMeta(klayer);
    }
    result.setLayer(klayer);
    result.setPartitioner(partitioner);
    return (T) result;
  }

}
