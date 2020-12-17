package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.model.TimedPoint;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class TrajectoryPointLayerDemo {

  private static final Logger logger = LoggerFactory.getLogger(TrajectoryPointLayerDemo.class);

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
    Field orderIdf = new Field("orderId","订单ID", 0, FieldType.NORMAL_FIELD);
    Field statusf = new Field("status", "订单状态", 2, FieldType.NORMAL_FIELD);
    Field riderIdf = new Field("riderId", "骑手ID",3, FieldType.NORMAL_FIELD);
    Field dayf = new Field("day", "订单日期" ,4, FieldType.NORMAL_FIELD);

    Field[] fields = new Field[]{orderIdf, statusf, riderIdf, dayf};

    Field timef = Term.FIELD_DEFAULT_TIME;
    timef.setIndex(1);

    FileLayerReaderConfig readerConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), file, LayerType.TRAJECTORY_POINT_LAYER);
    readerConfig.setTimeField(timef);
    readerConfig.setAttributes(fields);

    FileLayerReader<TrajectoryPointLayer> reader = new FileLayerReader<TrajectoryPointLayer>(ss, readerConfig);

    // read data from source
    TrajectoryPointLayer layer = reader.read();

    layer.cache();

    List<Tuple2<String, TimedPoint>> m0 = layer.collect();
    logger.info("======================= M0 ========================");
    logger.info(String.format("result size: %d", m0.size()));
    for (Tuple2<String, TimedPoint> t: m0) {
      logger.info(t._2.toString());
    }

    // filter data to target spatial area
    Envelope e = new Envelope(120.0826484129990348, 120.2443047286111408, 30.2467093379181975, 30.3120984094017416);

    // construct spatial index
    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.RECT_GRID);
    IndexedLayer<TrajectoryPointLayer> til = si.index(layer);
    til = til.query(JTS.toGeometry(e));
    TrajectoryPointLayer layer0 = til.toLayer();
    List<Tuple2<String, TimedPoint>> m1 = layer0.collect();
    logger.info("======================= M1 ========================");
    logger.info(String.format("result size: %d", m1.size()));
    for (Tuple2<String, TimedPoint> t: m1) {
      logger.info(t._2.toString());
    }

    DistributeSpatialIndex si2 = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.RTREE);
    IndexedLayer<TrajectoryPointLayer> til2 = si2.index(layer);
    til2 = til2.query(JTS.toGeometry(e));
    TrajectoryPointLayer layer2 = til2.toLayer();
    List<Tuple2<String, TimedPoint>> m2 = layer2.collect();
    logger.info("======================= M2 ========================");
    logger.info(String.format("result size: %d", m2.size()));
    for (Tuple2<String, TimedPoint> t: m2) {
      logger.info(t._2.toString());
    }

    layer.unpersist();

    // write data to sink
//    MongoLayerWriterConfig writerConfig = new MongoLayerWriterConfig("mongodb://localhost:27017", "dwd", "index_rider_point");
//    LayerWriter writer = new MongoLayerWriter(ss, writerConfig);
//    writer.write(layer);

    // close the project
    ss.close();

  }

}
