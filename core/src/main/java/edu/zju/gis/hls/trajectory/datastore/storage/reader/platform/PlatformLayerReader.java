package edu.zju.gis.hls.trajectory.datastore.storage.reader.platform;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Hu
 * @date 2020/7/3
 **/
public class PlatformLayerReader<T extends Layer> extends LayerReader<T> {

  private static final Logger logger = LoggerFactory.getLogger(PlatformLayerReader.class);

  @Getter
  @Setter
  private PlatformLayerReaderConfig readerConfig;

  public PlatformLayerReader(SparkSession ss, LayerType layerType, PlatformLayerReaderConfig readerConfig) {
    super(ss, layerType);
    this.readerConfig = readerConfig;
  }

  @Override
  public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

}
