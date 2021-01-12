package edu.zju.gis.hls.trajectory.analysis.index.rectGrid;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Serializable;


/**
 * @author Hu
 * @date 2019/12/16
 * 构建均匀格网索引的图层
 **/
@Getter
@Setter
@Slf4j
public class RectGridIndex implements DistributeSpatialIndex, Serializable {

  private RectGridIndexConfig c;

  public RectGridIndex() {
    this.c = new RectGridIndexConfig();
  }

  public RectGridIndex(RectGridIndexConfig c) {
    this.c = c;
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer) {
   return this.index(layer, true);
  }

  /**
   * 构建四叉树索引
   * HINT：每次构建完KeyedIndexLayer都要重新 repartition
   * @param layer
   * @return
   */
  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges) {
    return this.index(layer, withKeyRanges, -1);
  }

  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges, int numPartitions) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    PyramidConfig pc = new PyramidConfig.PyramidConfigBuilder().setCrs(crs).setZLevelRange(Term.QUADTREE_MIN_Z, Term.QUADTREE_MAX_Z).setBaseMapEnv(CrsUtils.getCrsEnvelope(crs)).build(true);
    RectGridIndexLayer<L> result = new RectGridIndexLayer<L>();
    RectGridPartitioner partitioner = new RectGridPartitioner(pc, c, numPartitions);
    L l = (L) layer.flatMapToLayer(partitioner);
    l.makeSureCached();
    if (numPartitions == -1) partitioner.setPartitionNum(l.distinctKeys().size());
    L klayer = (L) l.partitionByToLayer(partitioner);
    l.unpersist();
    if (withKeyRanges) {
      partitioner.collectPartitionMeta(klayer);
    }
    result.setLayer(klayer);
    result.setPartitioner(partitioner);
    return (T) result;
  }

}
