package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * @author Hu
 * @date 2020/7/7
 * PgLayerReader 测试
 * TODO
 * (1) timestamp 类型读取测试
 **/
@Slf4j
public class PgLayerIOExample {

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("PostgreSQL Layer Reader Demo")
      .master("local[4]")
      .getOrCreate();

    String source = "jdbc:postgresql://localhost:5432/postgres";
    String user = "postgres";
    String password = "root";
    String schema = "public";
    String dbtable = "test";

    Field fid = new Field("id", FieldType.ID_FIELD);
    Field shapeField = new Field("shape", FieldType.SHAPE_FIELD);

    // set up layer reader config
    PgLayerReaderConfig config = new PgLayerReaderConfig("point_layer_test", source, LayerType.MULTI_POLYGON_LAYER);
    config.setDbtable(dbtable);
    config.setUsername(user);
    config.setPassword(password);
    config.setSchema(schema);
    config.setIdField(fid);
    config.setShapeField(shapeField);

    // read layer
    PgLayerReader<MultiPolygonLayer> layerReader = new PgLayerReader<MultiPolygonLayer>(ss, config);
    MultiPolygonLayer layer = layerReader.read();

    // check if success
    layer.makeSureCached();
    log.info("Layer count: " + layer.count());

    List<Tuple2<String, MultiPolygon>> features = layer.collect();
    features.forEach(x->log.info(x._2.toJson()));

    PgLayerWriterConfig wconfig = new PgLayerWriterConfig(source,"public", "test2", user, password);
    PgLayerWriter layerWriter = new PgLayerWriter(ss, wconfig);
    layerWriter.write(layer);

    layer.release();
  }

}
