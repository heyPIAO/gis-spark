package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;

/**
 * @author Hu
 * @date 2020/7/1
 **/
public class PgLayerReaderConfig extends LayerReaderConfig {

  public PgLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
