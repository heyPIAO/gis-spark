package edu.zju.gis.hls.trajectory.datastore.storage.reader.platform;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Hu
 * @date 2020/7/3
 **/
@Slf4j
public class PlatformLayerReader<T extends Layer> extends LayerReader<T> {

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
