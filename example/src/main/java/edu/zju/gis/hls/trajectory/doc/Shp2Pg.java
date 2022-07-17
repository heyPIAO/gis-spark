package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import edu.zju.gis.hls.trajectory.sql.util.SparkSqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.xml.bind.ValidationException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

@Slf4j
public class Shp2Pg {

    private final static String LOCAL_FILE = "local";
    private final static String OSS_FILE = "oss";
    private final static String FID_FIELD_NAME = "id";
    private final static String SHAPE_FIELD_NAME = "the_geom";
    private final static String APP_NAME = "shp2Pg";
    private final static String ATTRIBUTES_SPLIT = ",";
    private final static String ATTRIBUTES_ALL = "ALL";
    private final static String TRUE = "TRUE";
    private final static String FALSE = "FALSE";

    public static void main(String[] args) throws Exception {

        String source, user, password, schema, tableName, sourcePath, mode, attrs, crs, charset, index;
        try {
            // ============================ database info ============================
            source = args[0]; // e.g. jdbc:postgresql://gp-bp1dzd3e839uf010eo-master.gpdb.rds.aliyuncs.com:5432/fuxi
            user = args[1]; // e.g. postgres
            password = args[2]; // e.g. ikz0s0FMXoXbOtrT
            schema = args[3]; // e.g. public
            tableName = args[4]; // e.g. vector_test_upb_new3
            // ============================ file info ============================
            sourcePath = args[5]; // e.g. /Users/wangluoluo/Downloads/shp_temp/china.shp
            // e.g. http://s3a/oss-cn-hangzhou.aliyuncs.comFUXI_S3_SPLITFUXI_S3_SPLITLTAI4GHXBwHSbpQ1zUeUcGgyFUXI_S3_SPLITwsP1eUpKguG02YuTUeZ70Cf0TXjetAFUXI_S3_SPLITdeepengineFUXI_S3_SPLITworkspace/ADMIN/userData/0DDE/JJZ/upb_new.shp
            mode = args[6]; // e.g. oss; local
            // ============================ layer info ===========================
            attrs = args[7]; // NAME,ATTR1,ATTR2
            crs = args[8]; // EPSG:4326, null
            charset = args[9]; // UTF-8,
            index = args[10]; // create index or not? true,false
        } catch (Exception e) {
            throw new ValidationException("The num of arguments has errors.");
        }

//        SparkSession ss = SparkSqlUtil.createSparkSessionWithSqlExtent(SparkSessionType.LOCAL, APP_NAME);
        SparkSession ss = SparkSqlUtil.createSparkSessionWithSqlExtent(SparkSessionType.REMOTE, APP_NAME);
//        source = "jdbc:postgresql://gp-bp1dzd3e839uf010eo-master.gpdb.rds.aliyuncs.com:5432/fuxi";
//        source ="jdbc:postgresql://pgm-bp18r1dq5gq2b8w7po.pg.rds.aliyuncs.com:1921/fuxi";
//        user = "postgres";
//        user = "deepengine_1";
//        password = "ikz0s0FMXoXbOtrT";
//        password = "upxTP1oR5SIbeQKG";
//        schema = "public";
//        tableName = "vector_test_upb_new2";

        Field fid = new Field(FID_FIELD_NAME, FieldType.ID_FIELD);
        Field shapeField = new Field(SHAPE_FIELD_NAME,
                org.locationtech.jts.geom.Geometry.class.getName(), FieldType.SHAPE_FIELD);

        String layerName = sourcePath.substring(sourcePath.lastIndexOf('/') + 1, sourcePath.lastIndexOf('.'));
        ShpLayerReaderConfig readerConfig = new ShpLayerReaderConfig(layerName, sourcePath, LayerType.GEOMETRY_LAYER);
        readerConfig.setIdField(fid);
        readerConfig.setShapeField(shapeField);
        if (!attrs.equalsIgnoreCase(ATTRIBUTES_ALL)) {
            String[] split = attrs.split(ATTRIBUTES_SPLIT);
            Field[] fieldArr = new Field[split.length];
            for (int i = 0; i < split.length; i++) {
                fieldArr[i] = new Field(split[i].trim());
            }
            readerConfig.setAttributes(fieldArr);
        }

        ShpLayerReader<Layer> layerReader = new ShpLayerReader<>(ss, readerConfig);

        Layer layer;
        switch (mode) {
            case LOCAL_FILE:
                layer = layerReader.read(crs, charset);
                break;
            case OSS_FILE:
                layer = layerReader.readOSS(crs, charset);
                break;
            default:
                throw new ValidationException("No such mode supported.");
        }

        layer.makeSureCached();
        PgLayerWriterConfig writerConfig = new PgLayerWriterConfig(source, schema, tableName, user, password);
        PgLayerWriter layerWriter = new PgLayerWriter(ss, writerConfig);
        layerWriter.write(layer);

        layer.release();

        // create spatial index
        if (index.equalsIgnoreCase(TRUE)) {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(
                    source, user, password);
            Statement stmt = connection.createStatement();
            String sql = "CREATE INDEX idx_geom_" + tableName + " ON " + tableName + " USING gist(the_geom);";
            try {
                stmt.execute(sql);
                log.info("create spatial index on the_geom successfully.");
            } catch (Exception e) {
                log.info("create spatial index on the_geom failed.");
            }

            stmt.close();
            connection.close();
        }
    }

}
