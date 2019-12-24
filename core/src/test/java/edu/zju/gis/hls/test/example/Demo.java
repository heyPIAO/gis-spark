package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriterConfig;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class Demo {

  private static final Logger logger = LoggerFactory.getLogger(Demo.class);

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
    Field orderIdf = new Field("orderId","订单ID", 0, FieldType.NORMA_FIELD);
    Field statusf = new Field("status", "订单状态", 2, FieldType.NORMA_FIELD);
    Field riderIdf = new Field("riderId", "骑手ID",3, FieldType.NORMA_FIELD);
    Field dayf = new Field("day", "订单日期" ,4, FieldType.NORMA_FIELD);

    Field[] fields = new Field[]{orderIdf, statusf, riderIdf, dayf};

    Field timef = Term.FIELD_DEFAULT_TIME;
    timef.setIndex(1);

    FileLayerReaderConfig readerConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), file, LayerType.TRAJECTORY_POINT_LAYER);
    readerConfig.setTimeField(timef);
    readerConfig.setAttributes(fields);

    FileLayerReader<TrajectoryPointLayer> reader = new FileLayerReader<TrajectoryPointLayer>(ss, readerConfig);

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

    // write data to sink
    MongoLayerWriterConfig writerConfig = new MongoLayerWriterConfig("mongodb://localhost:27017", "dwd", "index_rider_point");
    LayerWriter writer = new MongoLayerWriter(ss, writerConfig);
    writer.write(layer);

    // close the project
    ss.close();

  }

}
