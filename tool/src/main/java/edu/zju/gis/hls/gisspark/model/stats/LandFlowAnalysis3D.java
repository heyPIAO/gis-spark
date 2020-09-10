package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;

/**
 * @author Hu
 * @date 2020/9/10
 **/
public class LandFlowAnalysis3D extends BaseModel<LandFlowAnalysis3DArgs> {

  private static Integer DEFAULT_INDEX_LEVEL = 14;

  public LandFlowAnalysis3D(SparkSessionType type, String[] args) {
    super(type, args);
  }

  @Override
  protected void run() throws Exception {

    LayerReader layer1Reader = LayerFactory.getReader(this.ss, this.arg.getLayer1ReaderConfig());
    LayerReader layer2Reader = LayerFactory.getReader(this.ss, this.arg.getLayer2ReaderConfig());

    MultiPolygonLayer layer1 = (MultiPolygonLayer)layer1Reader.read();
    MultiPolygonLayer layer2 = (MultiPolygonLayer)layer2Reader.read();

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(DEFAULT_INDEX_LEVEL, false));

    KeyIndexedLayer<MultiPolygonLayer> indexedLayer1 = si.index(layer1);
    KeyIndexedLayer<MultiPolygonLayer> indexedLayer2 = si.index(layer2);

    KeyIndexedLayer<MultiPolygonLayer> resultLayer = indexedLayer1.intersect(indexedLayer2, this.arg.getAttrReserved());

    LayerWriter writer = LayerFactory.getWriter(this.ss, this.arg.getWriterConfig());

    writer.write(resultLayer);
  }

  public static void main(String[] args) throws Exception {
    LandFlowAnalysis3D analysis3D = new LandFlowAnalysis3D(SparkSessionType.LOCAL, args);
    analysis3D.exec();
  }
}
