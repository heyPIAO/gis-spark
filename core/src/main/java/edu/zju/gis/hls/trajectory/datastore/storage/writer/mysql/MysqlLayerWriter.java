package edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql.MysqlLayerWriterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Zhou
 * @date 2020/7/1
 * 将图层数据写出到 Mysql 数据库
 **/
@Slf4j
public class MysqlLayerWriter<T extends Layer> extends LayerWriter<Row> {

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
        JavaRDD<Row> rdd = ((JavaRDD<Tuple2<String, Feature>>)(layer.rdd().toJavaRDD())).map(x->x._2).map(x->transform(x));
        Dataset<Row> df = this.ss.createDataFrame(rdd, this.getLayerStructType(layer));
        df.write()
                .format("jdbc")
                .option("url", config.getSinkPath())
                .option("dbtable", config.getTablename())
                .option("user", config.getUsername())
                .option("password", config.getPassword())
                .save();
    }

    /**
     * 根据Layer的元数据信息获取Dataset的StructType
     * structtype的顺序默认：ID为第1列，SHAPE为最后一列
     */
    private StructType getLayerStructType(Layer layer) {
        Map<Field, Object> layerAttrs = layer.getMetadata().getExistAttributes();

        List<StructField> sfsl = new LinkedList<>();
        sfsl.add(convertFieldToStructField(layer.getMetadata().getIdField()));

        Field[] attrs = new Field[layerAttrs.size()];
        layerAttrs.keySet().toArray(attrs);
        for (int i=0; i<layerAttrs.size(); i++) {
            if (attrs[i].getFieldType().equals(FieldType.ID_FIELD) || attrs[i].getFieldType().equals(FieldType.SHAPE_FIELD)) {
                continue;
            }
            sfsl.add(convertFieldToStructField(attrs[i]));
        }

        sfsl.add(convertFieldToStructField(layer.getMetadata().getShapeField()));
        StructField[] sfs = new StructField[sfsl.size()];
        sfsl.toArray(sfs);
        return new StructType(sfs);
    }

    /**
     * 根据 Field 的元数据信息转成 StructField
     */
    private StructField convertFieldToStructField(Field field) {
        return new StructField(field.getName(),
                Field.converFieldTypeToDataType(field),
                !(field.getFieldType().equals(FieldType.ID_FIELD) || field.getFieldType().equals(FieldType.SHAPE_FIELD)),
                Metadata.empty());
    }
}
