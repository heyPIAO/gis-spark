package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql.MysqlLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql.MysqlLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

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

        // set up layer reader config
        MysqlLayerReaderConfig config = new MysqlLayerReaderConfig("mysql_layer_test1", source, LayerType.POLYGON_LAYER);
        config.setDbtable(dbtable);
        config.setUsername(user);
        config.setPassword(password);
        config.setIdField(fid);
        config.setShapeField(shapeField);

        // read layer
        MysqlLayerReader<PolygonLayer> layerReader = new MysqlLayerReader<PolygonLayer>(ss, config);
        PolygonLayer layer = layerReader.read();

        // check if success
        layer.makeSureCached();
        layer.count();
        log.info("Layer count: " + layer.count());

        layer.release();

    }

}
