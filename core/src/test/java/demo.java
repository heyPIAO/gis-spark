import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.model.PointFeature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class demo {

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

    Map<String, String> headerIndex = new HashMap<>();
    headerIndex.put("orderId", "0");
    headerIndex.put("status", "2");
    headerIndex.put("riderId", "3");
    headerIndex.put("day", "6");
    Gson gson = new Gson();
    prop.setProperty(ReaderConfig.HEADER_INDEX, gson.toJson(headerIndex));

    LayerReader<PointLayer> reader = LayerReaderFactory.getReader(ss, FeatureType.POINT, SourceType.FILE);
    reader.setProp(prop);

    // read data
    PointLayer layer = reader.read();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("orderId", "订单ID");
    attributes.put("status", "订单状态");
    attributes.put("riderId", "骑手ID");
    attributes.put("day", "订单日期");
    layer.setAttributes(attributes);

    PointLayer shifedPointLayer = layer.shift(0.01, 0.01);

    // TODO write data to sink
    shifedPointLayer.collect().forEach(new Consumer<Tuple2<String, PointFeature>>() {
      @Override
      public void accept(Tuple2<String, PointFeature> t) {
        System.out.println(String.format("uuid = %s, %s", t._1(), t._2().toString()));
      }
    });
  }

}
