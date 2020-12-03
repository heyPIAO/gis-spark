package edu.zju.gis.hls.trajectory.doc;
//
//import com.google.gson.Gson;
//import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
//import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryODLayer;
//import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
//import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPolylineLayer;
//import edu.zju.gis.hls.trajectory.analysis.util.SparkUtil;
//import edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm;
//import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
//import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
//import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
//import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriter;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * @author Hu
// * @date 2019/11/8
// **/
public class TrajODDemo {
//
  public static void main(String[] args) throws Exception {
//
//    // setup spark environment
//    SparkConf sc = new SparkConf();
//    // SparkSession ss = SparkUtil.createRemoteSparkSession("trajOD", sc);
//    // SparkSession ss = SparkUtil.createLocalSparkSession("trajOD", sc);
//    SparkSession ss = SparkUtil.createSparkSession("trajOD", sc);
//    // set up data source
//    String file = "file:///root/hls/data/dwd/hz/20170920_wkt.txt";
//    // String file = "file:///D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\杭州_骑手\\20170920_wkt.txt";
//    // String file = "file:///D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\rider\\228493884_wkt.txt";
//
//    // set up layer reader
//    Properties prop = new Properties();
//    prop.setProperty(ReaderConfigTerm.FILE_PATH, file);
//    prop.setProperty(ReaderConfigTerm.SEPARATOR, "\t");
//
//    Map<String, String> headerIndex = new HashMap<>();
//    headerIndex.put("orderId", "0");
//    headerIndex.put("status", "2");
//    headerIndex.put("riderId", "3");
//    headerIndex.put("day", "4");
//    Gson gson = new Gson();
//    prop.setProperty(ReaderConfigTerm.HEADER_INDEX, gson.toJson(headerIndex));
//    prop.setProperty(ReaderConfigTerm.TIME_INDEX, "1");
//
//    LayerReader<TrajectoryPointLayer> reader = LayerReaderFactory.getReader(ss, FeatureType.TRAJECTORY_POINT, SourceType.FILE);
//    reader.setProp(prop);
//
//    // read data from source
//    TrajectoryPointLayer layer = reader.read();
//    Map<String, String> attributes = new HashMap<>();
//    attributes.put("orderId", "订单ID");
//    attributes.put("status", "订单状态");
//    attributes.put("riderId", "骑手ID");
//    attributes.put("day", "订单日期");
//    layer.setAttributes(attributes);
//
//    TrajectoryPolylineLayer polylineLayer = layer.convertToPolylineLayer(JavaSparkContext.fromSparkContext(ss.sparkContext()), "orderId", "status", "riderId", "day");
//    TrajectoryODLayer odLayer = polylineLayer.extractOD();
//    odLayer.getMetadata().setLayerName("TrajectoryOD");
//
//    // write data to sink
//    Properties sinkProp = new Properties();
//    sinkProp.setProperty("uri", "mongodb://192.168.1.86:27017");
//    sinkProp.setProperty("database", "dwd");
//    sinkProp.setProperty("collection", "traj_od");
//    LayerWriter writer = new MongoLayerWriter(ss);
//    writer.write(odLayer, sinkProp);
//
//    ss.close();
//
  }
//
//
}
