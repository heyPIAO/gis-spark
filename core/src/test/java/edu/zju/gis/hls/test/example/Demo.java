package edu.zju.gis.hls.test.example;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.MongoDBLayerWriter;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class demo {

  private static final Logger logger = LoggerFactory.getLogger(demo.class);

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("trajectory_data_loader")
      .master("local[4]")
      .getOrCreate();

    // set up data source
    String file = "D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\杭州_骑手\\head_20_20170920_wkt.txt";

    // set up layer reader
    Properties prop = new Properties();
    prop.setProperty(ReaderConfig.FILE_PATH, file);

    Map<String, String> headerIndex = new HashMap<>();
    headerIndex.put("orderId", "0");
    headerIndex.put("status", "2");
    headerIndex.put("riderId", "3");
    headerIndex.put("day", "4");
    Gson gson = new Gson();
    prop.setProperty(ReaderConfig.HEADER_INDEX, gson.toJson(headerIndex));
    prop.setProperty(ReaderConfig.TIME_INDEX, "1");

    LayerReader<TrajectoryPointLayer> reader = LayerReaderFactory.getReader(ss, FeatureType.TRAJECTORY_POINT, SourceType.FILE);
    reader.setProp(prop);

    // read data from source
    TrajectoryPointLayer layer = reader.read();

    // construct spatial index
    SpatialIndex si = SpatialIndexFactory.getSpatialIndex(IndexType.QUADTREE);
    IndexedLayer<TrajectoryPointLayer> til = si.index(layer);

    // filter data to target spatial area
    Envelope e = new Envelope(120.0826484129990348, 120.2443047286111408, 30.2467093379181975, 30.3120984094017416);
    til = til.query(JTS.toGeometry(e));

    layer = til.toLayer();

    List m = layer.collect();
    logger.info(String.format("result size: %d", m.size()));

    Map<String, String> attributes = new HashMap<>();
    attributes.put("orderId", "订单ID");
    attributes.put("status", "订单状态");
    attributes.put("riderId", "骑手ID");
    attributes.put("day", "订单日期");
    layer.setAttributes(attributes);

    // write data to sink
    Properties sinkProp = new Properties();
    sinkProp.setProperty("uri", "mongodb://localhost:27017");
    sinkProp.setProperty("database", "dwd");
    sinkProp.setProperty("collection", "index_rider_point");
    LayerWriter writer = new MongoDBLayerWriter(ss);
    writer.write(layer, sinkProp);

    // close the project
    ss.close();

  }

}
