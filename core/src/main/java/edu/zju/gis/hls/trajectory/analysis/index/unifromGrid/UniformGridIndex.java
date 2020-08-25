package edu.zju.gis.hls.trajectory.analysis.index.unifromGrid;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.DistributeSpatialPartioner;
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
public class UniformGridIndex implements DistributeSpatialIndex, Serializable {

  private UniformGridIndexConfig c;

  public UniformGridIndex() {
    this.c = new UniformGridIndexConfig();
  }

  public UniformGridIndex(UniformGridIndexConfig c) {
    this.c = c;
  }

  /**
   * 构建四叉树索引
   * HINT：每次构建完KeyedIndexLayer都要重新 repartition
   * @param layer
   * @return
   */
  @Override
  public <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    PyramidConfig pc = new PyramidConfig.PyramidConfigBuilder().setCrs(crs).setzLevelRange(Term.QUADTREE_MIN_Z, Term.QUADTREE_MAX_Z).setBaseMapEnv(CrsUtils.getCrsEnvelope(crs)).build();
    UniformGridIndexLayer result = new UniformGridIndexLayer(pc, c);
    UniformGridPartitioner partitioner = new UniformGridPartitioner(pc, c, layer.getNumPartitions());
    Layer klayer = layer.flatMapToLayer(DistributeSpatialPartioner.getPreProcessFunction(partitioner,c.isClip()))
      .partitionByToLayer(partitioner);
    klayer.makeSureCached();
    partitioner.collectPartitionMeta(klayer);
    result.setLayer(klayer);
    result.setPartitioner(partitioner);
    return (T) result;
  }

}
