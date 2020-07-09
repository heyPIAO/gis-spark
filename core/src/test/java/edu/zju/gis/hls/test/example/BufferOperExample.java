package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.operate.BufferOperator;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Hu
 * @date 2020/7/8
 **/
public class BufferOperExample {

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
    PgLayerReaderConfig config = new PgLayerReaderConfig("multipolygon_layer_test", source, LayerType.MULTI_POLYGON_LAYER);
    config.setDbtable(dbtable);
    config.setUsername(user);
    config.setPassword(password);
    config.setSchema(schema);
    config.setIdField(fid);
    config.setShapeField(shapeField);

    // read layer
    PgLayerReader<MultiPolygonLayer> layerReader = new PgLayerReader<MultiPolygonLayer>(ss, config);
    MultiPolygonLayer layer = layerReader.read();

    layer.makeSureCached();
    layer.print();

    BufferOperator bo = new BufferOperator(0.02);
    MultiPolygonLayer bufferedLayer = (MultiPolygonLayer) bo.operate(layer);
    bufferedLayer.print();

    ss.stop();
    ss.close();
  }


}
