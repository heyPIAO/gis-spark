package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Hu
 * @date 2020/7/1
 **/
public class PgLayerReader <T extends Layer> extends LayerReader<T> {

  public PgLayerReader(SparkSession ss, LayerType layerType) {
    super(ss, layerType);
  }

  @Override
  public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
