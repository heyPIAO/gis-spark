package edu.zju.gis.hls.vector;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolylineLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import edu.zju.gis.hls.trajectory.sql.util.SparkSqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

@Slf4j
public class ShpToPgExample {
    public static void main(String[] args) throws Exception {

        SparkSession ss = SparkSqlUtil.createSparkSessionWithSqlExtent(SparkSessionType.LOCAL, "shp2pg");

        String source = "jdbc:postgresql://gp-bp1dzd3e839uf010eo-master.gpdb.rds.aliyuncs.com:5432/fuxi";
//        source ="jdbc:postgresql://pgm-bp18r1dq5gq2b8w7po.pg.rds.aliyuncs.com:1921/fuxi";
        String user = "postgres";
//        user = "deepengine_1";
        String password = "ikz0s0FMXoXbOtrT";
//        password = "upxTP1oR5SIbeQKG";
        String schema = "public";
        String tableName = "vector_test_upb_new2";

        Field fid = new Field("id", FieldType.ID_FIELD);
        Field shapeField = new Field("shape", org.locationtech.jts.geom.Geometry.class.getName(), FieldType.SHAPE_FIELD);

        String localSourcePath = "/Users/wangluoluo/Downloads/shp_temp/china.shp";
        String ossSourcePath = "http://s3a/oss-cn-hangzhou.aliyuncs.comFUXI_S3_SPLITFUXI_S3_SPLITLTAI4GHXBwHSbpQ1zUeUcGgyFUXI_S3_SPLITwsP1eUpKguG02YuTUeZ70Cf0TXjetAFUXI_S3_SPLITdeepengineFUXI_S3_SPLITworkspace/ADMIN/userData/0DDE/JJZ/upb_new.shp";
        ShpLayerReaderConfig readerConfig = new ShpLayerReaderConfig("upb_new",
                ossSourcePath, LayerType.GEOMETRY_LAYER);
        readerConfig.setIdField(fid);
        readerConfig.setShapeField(shapeField);

        ShpLayerReader<Layer> layerReader = new ShpLayerReader<>(ss, readerConfig);
        Layer layer = layerReader.readOSS("EPSG:4326", "UTF-8");

        layer.makeSureCached();
        log.info("Layer count: " + layer.count());

//        List<Tuple2<String, Feature>> features = layer.collect();
//        features.forEach(x -> log.info(x._2.toJson()));

        PgLayerWriterConfig writerConfig = new PgLayerWriterConfig(source, schema, tableName, user, password);
        PgLayerWriter layerWriter = new PgLayerWriter(ss, writerConfig);
        layerWriter.write(layer);

        layer.release();

    }
}
