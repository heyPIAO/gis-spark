package edu.zju.gis.hls.trajectory.datastore.storage.writer.pg;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
 * @author Hu
 * @date 2020/7/1
 * write layer data into postgis
 **/
@Slf4j
public class PgLayerWriter<T extends Layer> extends LayerWriter<Row> {

    @Getter
    @Setter
    private PgLayerWriterConfig config;

    public PgLayerWriter(SparkSession ss, PgLayerWriterConfig config) {
        super(ss);
        this.config = config;
    }

    @Override
    public Row transform(Feature feature) {
        Row row = new GenericRow(feature.toObjectArray());
        return row;
    }

    @Override
    public void write(Layer layer) {
        JavaRDD<Row> rdd = ((JavaRDD<Tuple2<String, Feature>>) (layer.rdd().toJavaRDD())).map(x -> x._2).map(x -> transform(x));
        StructType st = this.getLayerStructType(layer);
        Dataset<Row> df = this.ss.createDataFrame(rdd, st);
        //.mode(config.getSaveMode()) 选择Overwrite会导致分表失效，因此固定为append模式。
        df.cache();
        List<Row> r = df.collectAsList();
        df.printSchema();
        df.write()
                .format("jdbc")
                .option("url", config.getSinkPath())
                .option("dbtable", String.format("%s.%s", config.getSchema(), config.getTablename()))
                .option("user", config.getUsername())
                .option("password", config.getPassword())
                .mode(SaveMode.Append)
                .save();
    }

    /**
     * 根据Layer的元数据信息获取Dataset的StructType
     * structtype的顺序默认：ID为第1列，SHAPE为最后一列
     *
     * @param layer
     * @return
     */
    private StructType getLayerStructType(Layer layer) {
        Map<Field, Object> layerAttrs = layer.getMetadata().getExistAttributes();

        List<StructField> sfsl = new LinkedList<>();
        Field idField = layer.getMetadata().getIdField();
        if (idField == null) {
            idField = Term.FIELD_DEFAULT_ID;
        }
        sfsl.add(convertFieldToStructField(idField));

        Field[] attrs = new Field[layerAttrs.size()];
        layerAttrs.keySet().toArray(attrs);
        for (int i = 0; i < layerAttrs.size(); i++) {
            if (attrs[i].getFieldType().equals(FieldType.ID_FIELD)
                    || attrs[i].getFieldType().equals(FieldType.SHAPE_FIELD)) {
                continue;
            }
            sfsl.add(convertFieldToStructField(attrs[i]));
        }

        if (!(layer instanceof StatLayer)) {
            Field shapeField = layer.getMetadata().getShapeField();
            if (shapeField == null) {
                shapeField = Term.FIELD_DEFAULT_SHAPE;
            }
            sfsl.add(convertFieldToStructField(shapeField));
        }

        StructField[] sfs = new StructField[sfsl.size()];
        sfsl.toArray(sfs);
        return new StructType(sfs);
    }

    /**
     * 根据 Field 的元数据信息转成 StructField
     *
     * @param field
     * @return
     */
    private StructField convertFieldToStructField(Field field) {
        return new StructField(field.getName(),
                Field.converFieldTypeToDataType(field),
                !(field.getFieldType().equals(FieldType.ID_FIELD) || field.getFieldType().equals(FieldType.SHAPE_FIELD)),
                Metadata.empty());
    }

}
