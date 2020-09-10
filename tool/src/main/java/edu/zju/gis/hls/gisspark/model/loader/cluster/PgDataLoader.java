package edu.zju.gis.hls.gisspark.model.loader.cluster;

import edu.zju.gis.hls.gisspark.model.args.DataLoaderArgs;
import edu.zju.gis.hls.gisspark.model.base.Constant;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.config.MSConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.MSHelper;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.PgHelper;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author Hu
 * @date 2020/9/3
 **/
@Slf4j
public class PgDataLoader extends DataLoader<DataLoaderArgs> {

    private PgConfig pgConfig = new PgConfig();
    private MSConfig msConfig = new MSConfig();

    public PgDataLoader(String[] args) {
        super(args);
        this.initMsConfig();
    }

    public PgDataLoader(SparkSessionType type, String[] args) {
        super(type, args);
    }

    /**
     * （1）建表
     * （2）根据ID字段做citus分区表
     */
    @Override
    protected void prepare() {
        super.prepare();
        LayerReaderConfig readerConfig = LayerFactory.getReaderConfig(this.arg.getInput());
        PgLayerWriterConfig writerConfig = (PgLayerWriterConfig) LayerFactory.getWriterConfig(this.arg.getOutput());
        this.initPgConfig(writerConfig);
        PgHelper pgHelper = new PgHelper(this.pgConfig);
        switch (writerConfig.getSaveMode()) {
            case Overwrite:
                log.info("Drop TARGET TABLE: " + writerConfig.getTablename());
                log.info("CREATE TARGET TABLE: " + writerConfig.getTablename());
                pgHelper.runSQL(this.createTableSqlWithDrop(readerConfig, writerConfig));
                break;
            case Append:
                log.info("CREATE TARGET TABLE : " + writerConfig.getTablename());
                pgHelper.runSQL(this.createTableSql(readerConfig, writerConfig));
                break;
            default:
                break;
        }
        pgHelper.close();
    }

    /**
     * 存储元数据
     *
     * @param metadata
     */
    @Override
    protected void storeMetadata(LayerMetadata metadata) {
        super.storeMetadata(metadata);
        // TODO 将图层元数据信息存储到平台的 mysql 数据库中
//        MSHelper helper = new MSHelper(msConfig);
//        helper.close();
    }

    /**
     * 没有考虑时间字段，包括 time，startTime，endTime
     * TODO 添加构建分词索引
     *
     * @param rc
     */
    private String createTableSql(LayerReaderConfig rc, PgLayerWriterConfig wc) {
        Field[] fields = rc.getAttributes();
        Field idField = rc.getIdField();
        Field geomField = rc.getShapeField();
        String tableName = wc.getTablename();
        StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName + "( ");
        sb.append(getAttrFromField(idField) + ", \n");
        for (Field f : fields) {
            sb.append(getAttrFromField(f) + ", \n");
        }
        sb.append(getAttrFromField(geomField) + " \n");
        sb.append(");");
        return sb.toString();
    }

    private String createTableSqlWithDrop(LayerReaderConfig rc, PgLayerWriterConfig wc) {
        String tableName = wc.getTablename();
        String dropTable = "DROP TABLE IF EXISTS" + tableName + ";";
        String createTabelSql = createTableSql(rc, wc);
        return dropTable + createTabelSql;
    }

    private String distributeTableSql(LayerReaderConfig rc, PgLayerWriterConfig wc) {
        String tableName = wc.getTablename();
        Field idf = rc.getIdField();
        // example: SELECT create_distributed_table('taxi', 'GRID_NUM', 'hash');
        return String.format("SELECT create_distributed_table('%s', '%s', '%s');",
                tableName, idf.getName(), Constant.DEFAULT_PG_DISTRIBUTE_METHOD);
    }

    private String createSpatialIndexSql(LayerReaderConfig rc, PgLayerWriterConfig wc, CoordinateReferenceSystem targetCrs) {
        String tableName = wc.getTablename();
        Field shapeField = rc.getShapeField();
        String indexName = String.format("%s_%s_%s", "gist", shapeField.getName(), tableName);
        // example: CREATE INDEX gist_shape ON tb_polygon USING GIST(st_geomfromtext(shape, 4326));
        return String.format("CREATE INDEX %s ON %s USING GIST(st_geomfromtext(\"%s\", %d))", indexName, tableName, shapeField.getName(), Term.getEpsgCode(targetCrs));
    }

    private String getAttrFromField(Field f) {
        StringBuffer sb = new StringBuffer();
        if (f.getFieldType().equals(FieldType.ID_FIELD)) {
            sb.append("\""+f.getName() + "\" VARCHAR PRIMARY KEY NOT NULL");
        } else if (f.getFieldType().equals(FieldType.SHAPE_FIELD)) {
            sb.append("\""+f.getName() + "\"  VARCHAR NOT NULL");
        } else {
            if (!f.isNumeric()) {
                sb.append("\""+f.getName() + "\"  VARCHAR");
            } else {
                if (f.getType().equals(Integer.class.getName())) {
                    sb.append("\""+f.getName() + "\"  INTEGER");
                } else {
                    sb.append("\""+f.getName() + "\"  REAL");
                }
            }
        }
        return sb.toString();
    }

    private void initPgConfig(PgLayerWriterConfig config) {
        // sinkpath example: jdbc:postgresql://localhost:5432/postgres
        // TODO 用正则取出来
        String[] s = config.getSinkPath().split(":");
        this.pgConfig.setUrl(s[2].replace("//", ""));
        this.pgConfig.setPort(Integer.valueOf(s[3].split("/")[0]));
        this.pgConfig.setDatabase(s[3].split("/")[1]);
        this.pgConfig.setUsername(config.getUsername());
        this.pgConfig.setPassword(config.getPassword());
        this.pgConfig.setSchema(config.getSchema());
    }

    private void initMsConfig() {
        InputStream in = this.getClass().getResourceAsStream("/mysqlConfig.properties");
        Properties props = new Properties();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            props.load(inputStreamReader);
            this.msConfig.setUrl((String) (props.getOrDefault("url", msConfig.getUrl())));
            this.msConfig.setPort((Integer) props.getOrDefault("port", msConfig.getPort()));
            this.msConfig.setDatabase((String) props.getOrDefault("database", msConfig.getDatabase()));
            this.msConfig.setUsername((String) props.getOrDefault("username", msConfig.getUsername()));
            this.msConfig.setPassword((String) props.getOrDefault("password", msConfig.getPassword()));
        } catch (IOException e) {
            throw new GISSparkException("read mysql configuration failed: " + e.getMessage());
        }
    }

    /**
     * 建空间索引
     */
    @Override
    protected void finish() {
        super.finish();
        LayerReaderConfig readerConfig = LayerFactory.getReaderConfig(this.arg.getInput());
        PgLayerWriterConfig writerConfig = (PgLayerWriterConfig) LayerFactory.getWriterConfig(this.arg.getOutput());
        PgHelper pgHelper = new PgHelper(this.pgConfig);
        try {
            log.info("DISTRIBUTE TARGET TABLE");
            pgHelper.runSQL(this.distributeTableSql(readerConfig, writerConfig));
            log.info(String.format("CREATE SPATIAL INDEX FOR %s USING GIST", writerConfig.getTablename()));
            pgHelper.runSQL(this.createSpatialIndexSql(readerConfig, writerConfig, CRS.parseWKT(this.arg.getTargetCrs())));
        } catch (FactoryException e) {
            throw new GISSparkException(e.getMessage());
        } finally {
            pgHelper.close();
        }
    }

    public static void main(String[] args) throws Exception {
        PgDataLoader pgLoader = new PgDataLoader(SparkSessionType.LOCAL, args);
        pgLoader.exec();
    }

}
