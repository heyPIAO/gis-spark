package edu.zju.gis.hls.trajectory.datastore.dataloader.cluster;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPoint;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author Hu
 * @date 2019/9/19
 * 点我达轨迹数据ETL工具
 * 入库步骤：
 * 1. 从 txt 中读取轨迹点
 * 2. 将轨迹点根据 order_id 合并成轨迹并按 timestamp 排序
 * 3. 对于每一个轨迹根据轨迹清洗规则进行清洗
 * 4. 轨迹数据入库到mongoDB
 **/
public class LoaderExample extends Loader {

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("trajectory_data_loader")
      .master("local[1]")
      .getOrCreate();

    // set up data source
    String file = "D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\rider\\order_228493884_wkt.txt";

    // set up layer reader
    Properties prop = new Properties();
    prop.setProperty(ReaderConfig.FILE_PATH, file);
    prop.setProperty(ReaderConfig.TIME_INDEX, "4");

    Map<String, String> headerIndex = new HashMap<>();
    headerIndex.put("orderId", "0");
    headerIndex.put("status", "2");
    headerIndex.put("riderId", "3");
    headerIndex.put("day", "6");
    Gson gson = new Gson();
    prop.setProperty(ReaderConfig.HEADER_INDEX, gson.toJson(headerIndex));

    Map<String, String> attributeTypes = new HashMap<>();
    attributeTypes.put("orderId", Long.class.getName());
    attributeTypes.put("status", String.class.getName());
    attributeTypes.put("riderId", Long.class.getName());
    attributeTypes.put("day", String.class.getName());
    prop.setProperty(ReaderConfig.ATTRIBUTE_TYPE, gson.toJson(attributeTypes));

    LayerReader<TrajectoryPointLayer> reader = LayerReaderFactory.getReader(ss, FeatureType.TRAJECTORY_POINT, SourceType.FILE);
    reader.setProp(prop);

    // read data
    TrajectoryPointLayer layer = reader.read();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("orderId", "订单ID");
    attributes.put("status", "订单状态");
    attributes.put("riderId", "骑手ID");
    attributes.put("day", "订单日期");
    layer.setAttributes(attributes);

    layer.persist(StorageLevel.MEMORY_AND_DISK());

    // TODO write data to sink
    layer.collect().forEach(new Consumer<Tuple2<String, TrajectoryPoint>>() {
      @Override
      public void accept(Tuple2<String, TrajectoryPoint> t) {
        System.out.println(String.format("%s",t._2().toString()));
      }
    });

    System.out.println(layer.getMetadata());
  }

}
