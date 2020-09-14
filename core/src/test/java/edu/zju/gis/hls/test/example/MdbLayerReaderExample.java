package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.MdbLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.MdbLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Hu
 * @date 2020/9/14
 **/
@Slf4j
public class MdbLayerReaderExample {

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    SparkSession ss = SparkSession
      .builder()
      .appName("Mdb Layer Reader Demo")
      .master("local[1]")
      .getOrCreate();

    MdbLayerReaderConfig config = new MdbLayerReaderConfig();
    config.setLayerType(LayerType.MULTI_POLYGON_LAYER);
    config.setTargetLayerName("DLTB");
    config.setSourcePath("C:\\Users\\Hu\\Desktop\\test.mdb");

    MdbLayerReader<MultiPolygonLayer> dltbLayerReader = new MdbLayerReader<>(ss, config);
    MultiPolygonLayer dltbLayer = dltbLayerReader.read();

    // dltbLayer.cache();

    log.info(String.format("Layer Count: %d", dltbLayer.count()));
    // dltbLayer.print();

    ss.close();
    ss.stop();
  }

}
