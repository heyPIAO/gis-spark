package edu.zju.gis.hls.trajectory.datastore.storage.writer.pg;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.hls.dialect.GeometryDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.util.List;


/**
 * @author Hu
 * @date 2020/7/1
 * write layer data into postgis
 **/
@Slf4j
public class PgLayerWriter extends LayerWriter<Row> {

    @Getter
    @Setter
    private PgLayerWriterConfig config;

    public PgLayerWriter(SparkSession ss, PgLayerWriterConfig config) {
        super(ss);
        this.config = config;
    }

    @Override
    public Row transform(Feature feature) {
        return new GenericRow(feature.toObjectArray());
    }

    @Override
    public void write(Layer layer) {
        Dataset<Row> df = layer.toDataset(this.ss, true);
//        df.cache();
//        List<Row> r = df.collectAsList();
        JdbcDialects.registerDialect(new GeometryDialect());

        df.cache();
        df.printSchema();
        df.write()
//                .format(PostgresSource.class.getCanonicalName()) // datasource v3的写法，但是不好用
                .format("pg") // datasource v1
                .option("url", config.getSinkPath())
                .option("dbtable", String.format("%s.%s", config.getSchema(), config.getTablename()))
                .option("user", config.getUsername())
                .option("password", config.getPassword())
                .option("driver", config.getDriver())
                .mode(config.getSaveMode().equals(SaveMode.Append) ? SaveMode.Append : SaveMode.ErrorIfExists)
                .save();
        df.unpersist();
    }

}
