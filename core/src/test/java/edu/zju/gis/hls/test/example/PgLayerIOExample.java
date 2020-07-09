package edu.zju.gis.hls.test.example;

import edu.zju.gis.dbfg.queryserver.constant.PlatFormStorageType;
import edu.zju.gis.dbfg.queryserver.model.PgConnectInfo;
import edu.zju.gis.dbfg.queryserver.tool.DataFactory;
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

    public static void main(String[] args) throws Exception {
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
        features.forEach(x -> log.info(x._2.toJson()));

        PgLayerWriterConfig wconfig = new PgLayerWriterConfig(source, "public", "test2", user, password);
        PgLayerWriter layerWriter = new PgLayerWriter(ss, wconfig);
        layerWriter.write(layer);

        layer.release();
    }

    private static PgLayerReaderConfig setConfig_CZD() throws Exception {
        DataFactory pgDf = new DataFactory(PlatFormStorageType.PF, "2fa7d456-0d60-48ba-881b-bace9c47a005");
        PgConnectInfo pgConnectInfo = (PgConnectInfo) pgDf.getSchema();
        String source = pgConnectInfo.toString();
        String user = pgConnectInfo.getUserName();
        String password = pgConnectInfo.getPassword();
        String schema = pgConnectInfo.getSchemaName();
        String dbtable = pgConnectInfo.getTableName();
        String idFieldName = pgConnectInfo.getPkField();
        String idFieldType = pgConnectInfo.getPkType();
        String shapeFieldName = pgConnectInfo.getSpatialField();
        String shapeFieldType = pgConnectInfo.getSpatialType();
        String timeFieldName = pgConnectInfo.getTimeField();
        String timeFieldType = pgConnectInfo.getTimeType();
        String fileds = pgConnectInfo.getFields();

        Field gid = new Field(idFieldName, FieldType.ID_FIELD);
        //TODO 根据数据库字段类型，返回java类型; <需要补全>
        gid.setType(dbClass2JavaClass(idFieldType));
        Field shapeField = new Field(shapeFieldName, FieldType.SHAPE_FIELD);

        FeatureType featureType = FeatureType.valueOf(shapeFieldType);
        LayerType type = LayerType.findLayerType(featureType);

        PgLayerReaderConfig config = new PgLayerReaderConfig("point_layer_test", source, type);
        config.setDbtable(dbtable);
        config.setUsername(user);
        config.setPassword(password);
        config.setSchema(schema);
        config.setIdField(gid);
        config.setShapeField(shapeField);

        pgDf.close();

        return config;
    }

    private static Class dbClass2JavaClass(String fieldType) {
        if (fieldType.contains("int"))
            return Integer.class;
        return String.class;
    }

}
