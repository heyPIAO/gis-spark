package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql.MysqlLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql.MysqlLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql.MysqlLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql.MysqlLayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

/**
 * @author Zhou
 * @date 2020/7/20
 * MysqlLayerReader 测试
 **/

@Slf4j
public class MysqlIOExample {

    public static void main(String[] args) throws Exception {
        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("MySQL Layer Reader Demo")
                .master("local[4]")
                .getOrCreate();

        String source = "jdbc:mysql://localhost:3306/test_db?serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "566000";
        String dbtable = "test1";

        Field fid = new Field("id", FieldType.ID_FIELD);
        Field shapeField = new Field("wkt", FieldType.SHAPE_FIELD);
        // 除id，shape外，额外读取name字段
        Field nameField = new Field("name", FieldType.NORMA_FIELD);
        Field[] attributes = { nameField };

        // set up layer reader config
        MysqlLayerReaderConfig config = new MysqlLayerReaderConfig("mysql_layer_test1", source, LayerType.POLYGON_LAYER);
        config.setDbtable(dbtable);
        config.setUsername(user);
        config.setPassword(password);
        config.setIdField(fid);
        config.setShapeField(shapeField);
        config.setAttributes(attributes);


        // read layer
        MysqlLayerReader<PolygonLayer> layerReader = new MysqlLayerReader<PolygonLayer>(ss, config);
        PolygonLayer layer = layerReader.read();

        // check if success
        layer.makeSureCached();
        layer.count();
        log.info("Layer count: " + layer.count());

        List<Tuple2<String, Polygon>> features = layer.collect();
        features.forEach(x -> log.info(x._2.toJson()));

        MysqlLayerWriterConfig wconfig = new MysqlLayerWriterConfig(source, "test2", user, password);
        MysqlLayerWriter layerWriter = new MysqlLayerWriter(ss, wconfig);
        layerWriter.write(layer);
        log.info("Layer successfully writes!");

        layer.release();

    }

}
