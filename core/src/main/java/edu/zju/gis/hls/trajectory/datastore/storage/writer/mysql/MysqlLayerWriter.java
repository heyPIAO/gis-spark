package edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * @author Zhou
 * @date 2020/7/1
 * 将图层数据写出到 Mysql 数据库
 **/
@Slf4j
public class MysqlLayerWriter extends LayerWriter<Row> {

    @Getter
    @Setter
    private MysqlLayerWriterConfig config;

    public MysqlLayerWriter(SparkSession ss, MysqlLayerWriterConfig config) {
        super(ss);
        this.config = config;
    }

    @Override
    public Row transform(Feature feature) {
        return new GenericRow(feature.toObjectArray());
    }

    @Override
    public void write(Layer layer) {
        Dataset<Row> df = layer.toDataset(this.ss);
        df.write()
                .format("jdbc")
                .option("url", config.getSinkPath())
                .option("dbtable", config.getTablename())
                .option("user", config.getUsername())
                .option("password", config.getPassword())
                .save();
    }
}
