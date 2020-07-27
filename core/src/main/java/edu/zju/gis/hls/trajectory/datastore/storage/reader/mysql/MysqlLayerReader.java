package edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql.MysqlLayerReaderConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/**
 * @author Zhou
 * @date 2020/7/19
 **/

@ToString
@Slf4j
public class MysqlLayerReader <T extends Layer> extends LayerReader<T> {

    @Getter
    @Setter
    private MysqlLayerReaderConfig readerConfig;

    public MysqlLayerReader(SparkSession ss, LayerType layerType) {
        super(ss, layerType);
    }

    public MysqlLayerReader(SparkSession ss, MysqlLayerReaderConfig config) {
        super(ss, config.getLayerType());
        this.readerConfig = config;
    }

    @Override
    public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        // TODO 封装 read procedure，去掉以下重复的图层读取参数设置
        if (this.readerConfig == null) {
            throw new LayerReaderException("set layer reader config");
        }

        if (!this.readerConfig.check()) {
            throw new LayerReaderException("reader config is not set correctly");
        }

        Dataset<Row> df = this.ss.read().format("jdbc")
                .option("url", this.readerConfig.getSourcePath())
                .option("dbtable",  this.readerConfig.getDbtable())
                .option("user", this.readerConfig.getUsername())
                .option("password", this.readerConfig.getPassword())
                .option("continueBatchOnError",true)
                .option("pushDownPredicate", true) // 默认请求下推
                .load();

        log.info("check field meta info");
        checkFields(df.schema());
        log.info("field meda info check successful");

        log.info("select targeted fields");
        Field[] fields = this.readerConfig.getAllAttributes();
        List<String> names = new ArrayList<>();
        for (Field field: fields) {
            if (field.isExist()) {
                names.add(field.getName());
            }
        }

        log.info("select column names: " + String.join(", ", names));
        if (names.size() == 1) {
            df = df.select(names.get(0));
        } else {
            String first = names.get(0);
            names.remove(0);
            String[] tnames = new String[names.size()];
            names.toArray(tnames);
            df = df.select(first, tnames);
        }

        log.info(String.format("Transform Typed DataFrame to GISSpark %s Layer", this.readerConfig.getLayerType().name()));
        JavaRDD<Row> row = df.toJavaRDD();
        JavaPairRDD<String, Feature> features = row.mapToPair(new PairFunction<Row, String, Feature>(){
            @Override
            public Tuple2<String, Feature> call(Row row) throws Exception {

                // set up feature id
                Field idf = readerConfig.getIdField();
                String fid;
                if (idf.isExist()) {
                    fid = String.valueOf(row.get(row.fieldIndex(idf.getName())));
                } else {
                    fid = UUID.randomUUID().toString();
                }

                // set up geometry
                Field geof = readerConfig.getShapeField();
                String wktstr = row.getString(row.fieldIndex(geof.getName()));
                WKTReader wktReader = new WKTReader();
                Geometry geometry = wktReader.read(wktstr);

                // set up timestamp if exists
                Long timestamp = null;
                Field tf = readerConfig.getTimeField();
                if (tf.getIndex() != Term.FIELD_NOT_EXIST) {
                    timestamp = Long.valueOf(row.getString(row.fieldIndex(tf.getName())));
                }

                // set up starttime if exists
                Long startTime = null;
                Field stf = readerConfig.getStartTimeField();
                if (stf.isExist()) {
                    startTime = Long.valueOf(row.getString(row.fieldIndex(stf.getName())));
                }

                // set up endtime if exists
                Long endTime = null;
                Field etf = readerConfig.getEndTimeField();
                if (etf.isExist()) {
                    endTime = Long.valueOf(String.valueOf(row.get(row.fieldIndex(etf.getName()))));
                }

                // set feature attributes
                LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
                Field[] fs = readerConfig.getAttributes();
                for (Field f: fs) {
                    String name = f.getName();
                    String cname = f.getType();
                    Class c = Class.forName(cname);
                    attributes.put(f, Converter.convert(String.valueOf(row.get(row.fieldIndex(name))), c));
                }

                Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes, timestamp, startTime, endTime);
                return new Tuple2<>(feature.getFid(), feature);
            }
        });

        T layer = this.rddToLayer(features.rdd());
        LayerMetadata lm = new LayerMetadata();
        lm.setLayerId(readerConfig.getLayerId());
        lm.setCrs(readerConfig.getCrs());
        lm.setLayerName(readerConfig.getLayerName());

        // this.readerConfig.getIdField().setIndex(Term.FIELD_EXIST);
        lm.setAttributes(readerConfig.getAllAttributes());

        layer.setMetadata(lm);
        return layer;
    }

    private void checkFields(StructType st) throws LayerReaderException {
        Field[] fs = this.readerConfig.getAllAttributes();
        StructField[] sfs = st.fields();
        boolean[] flags = new boolean[fs.length];
        for (int i=0; i<fs.length; i++) {
            Field f = fs[i];
            if (!f.isExist()) {
                flags[i] = true;
                continue;
            }
            flags[i] = false;
            for (StructField sf: sfs) {
                if (sf.name().equals(f.getName()) && Field.equalFieldClass(sf.dataType(), f.getType())) {
                    flags[i] = true;
                    break;
                }
            }
        }
        for (int m=0; m<flags.length; m++) {
            if (!flags[m]) {
                throw new LayerReaderException(String.format("Field %s not exists in table %s ", fs[m].toString(), this.readerConfig.getDbtable()));
            }
        }
    }

    @Override
    public void close() throws IOException {
        log.info(String.format("MysqlLayerReader close: %s", readerConfig.toString()));
    }

}
