package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;

/**
 * 用于分区的R树索引
 * @author Hu
 * @date 2020/8/26
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class RTreeIndex implements DistributeSpatialIndex, Serializable {

  private RTreeIndexConfig c;

  public RTreeIndex() {
    this(new RTreeIndexConfig());
  }

  public RTreeIndex(RTreeIndexConfig config) {
    this.c = config;
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer) {
    return this.index(layer, true);
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges) {
    return this.index(layer, withKeyRanges, layer.context().defaultParallelism());
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges, int numPartitions) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    RTreePartitioner partitioner = new RTreePartitioner(numPartitions);
    partitioner.setConf(c);
    partitioner.setCrs(crs);
    layer.makeSureCached();
    List<Tuple2<String, Feature>> samples = layer.takeSample(false, c.getSampleSize());
    partitioner.build(samples);
    RTreeIndexLayer<L> result = new RTreeIndexLayer<L>(partitioner);
    L klayer = (L) layer.flatMapToLayer(partitioner).partitionByToLayer(partitioner);
    result.setLayer(klayer);
    result.setPartitioner(partitioner);
    return (T) result;
  }

}
