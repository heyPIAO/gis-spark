package edu.zju.gis.hls.trajectory.datastore.storage.reader.mongo;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;

/**
 * @author Hu
 * @date 2019/12/19
 **/
public class MongoLayerReaderConfig extends LayerReaderConfig {

  public MongoLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
