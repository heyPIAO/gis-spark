package edu.zju.gis.hls.trajectory.sql;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.sql.util.SparkSqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;
import java.util.Scanner;

/**
 * @author Hu
 * @date 2020/10/1
 **/
@Slf4j
public class SpatialFunctionTest {

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    SparkSession ss = SparkSqlUtil.createSparkSessionWithSqlExtent(SparkSessionType.LOCAL, "SpatialFunctionTest");

    String source = "jdbc:postgresql://localhost:5432/postgres";
    String user = "postgres";
    String password = "root";
    String schema = "public";
    String dbtable = "xzqh2019t";

    Field fid = new Field("id", FieldType.ID_FIELD);
    Field shapeField = new Field("shape", org.locationtech.jts.geom.Point.class.getName(), FieldType.SHAPE_FIELD);

    // set up layer reader config
    PgLayerReaderConfig config = new PgLayerReaderConfig("xzqh2019t", source, LayerType.POINT_LAYER);
    config.setDbtable(dbtable);
    config.setUsername(user);
    config.setPassword(password);
    config.setSchema(schema);
    config.setIdField(fid);
    config.setShapeField(shapeField);
    config.setAttributes(getAttributeFields());

    // read layer
    PgLayerReader<PointLayer> layerReader = new PgLayerReader<PointLayer>(ss, config);
    PointLayer layer = layerReader.read();

    layer.print();

    Dataset<Row> row = layer.toGeomDataset(ss);
    row.printSchema();

    row.createOrReplaceTempView("xzqh");

    Dataset<Row> m = ss.sql("select * from xzqh");
    m.explain(true);
    m.show();

    m = ss.sql("select * from xzqh where st_contains(st_makeBBOX(100, 120, 30, 35), shape)");
    m.explain(true);
    m.show();

    m = m.select("shape").filter("st_contains(st_makeBBOX(100, 120, 30, 35), shape)").filter("st_contains(st_makeBBOX(100, 130, 30, 40), shape)");
    m.explain(true);
    m.show();

    log.info("== SQL SPATIAL FUNCTION TEST START ==");
    Scanner sc = new Scanner(System.in);
    System.out.println("Command: ");
    String command = sc.nextLine();
    while(true) {
      if (command.toLowerCase().equals("exit")) break;
//      System.out.println(ss.sql(command).queryExecution().toString());
      Dataset<Row> t = ss.sql(command);
      t.queryExecution();
      t.explain();
      t.show();
      System.out.println("Command: ");
      command = sc.nextLine();
    }
    log.info("== SQL SPATIAL FUNCTION TEST STOP==");
    ss.close();
    ss.stop();
  }

  private static Field[] getAttributeFields() {
    Field pname = new Field("pname");
    Field pcode = new Field("pcode");
    Field cname = new Field("cname");
    Field ccode = new Field("ccode");
    Field dname = new Field("dname");
    Field dcode = new Field("dcode");
    Field tname = new Field("tname");
    Field tcode = new Field("tcode");
    Field vname = new Field("vname");
    Field vcode = new Field("vcode");
    Field type = new LongField("type");
    Field lon = new DoubleField("lon");
    Field lat = new DoubleField("lat");
    return new Field[]{pname, pcode, cname, ccode, dname, dcode,
      tname, tcode, vname, vcode, type, lon, lat};
  }

}
