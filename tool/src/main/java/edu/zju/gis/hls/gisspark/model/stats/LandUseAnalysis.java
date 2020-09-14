package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/9/10
 * 土地利用现状分析模型
 * Hint：默认 Extent Layer 不会有非常多图斑
 **/
public class LandUseAnalysis extends BaseModel<LandUseAnalysisArgs> {

  private static Integer DEFAULT_INDEX_LEVEL = 14;

  public LandUseAnalysis(SparkSessionType type, String[] args) {
    super(type, args);
  }

  @Override
  protected void run() throws Exception {
    LayerReader extendLayerReader = LayerFactory.getReader(this.ss, this.arg.getExtentReaderConfig());
    List<Geometry> extendGeometries = ((List<Tuple2<String, Feature>>)extendLayerReader.read().filterEmpty().collect())
      .stream().map(x->x._2.getGeometry()).collect(Collectors.toList());

    LayerReaderConfig targetLayerReaderConfig = this.arg.getTargetReaderConfig();
    SourceType st = SourceType.getSourceType(targetLayerReaderConfig.getSourcePath());
    if (st.equals(SourceType.PG) || st.equals(SourceType.CitusPG)) {
      // 基于范围图斑构造空间查询语句
      String sql = String.format("");
    }
    LayerReader targetLayerReader = LayerFactory.getReader(this.ss, this.arg.getTargetReaderConfig());
    Layer targetLayer = targetLayerReader.read();
    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(DEFAULT_INDEX_LEVEL, false));
    KeyIndexedLayer indexedLayer = si.index(targetLayer);
    Layer filteredLayer = indexedLayer.query(extendGeometries).toLayer();
    StatLayer statLayer = filteredLayer.aggregateByField(this.arg.getAggregateFieldName());

    LayerWriter writer = LayerFactory.getWriter(this.ss, this.arg.getStatsWriterConfig());
    writer.write(statLayer);
  }


  public static void main(String[] args) throws Exception {
    LandUseAnalysis analysis = new LandUseAnalysis(SparkSessionType.LOCAL, args);
    analysis.exec();
  }

}
