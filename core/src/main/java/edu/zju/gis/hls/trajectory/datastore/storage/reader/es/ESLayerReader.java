package edu.zju.gis.hls.trajectory.datastore.storage.reader.es;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/7/1
 **/
@Slf4j
@Getter
@Setter
public class ESLayerReader<T extends Layer> extends LayerReader<T> {

    private ESLayerReaderConfig readerConfig;

    public ESLayerReader(SparkSession ss, LayerType layerType) {
        super(ss, layerType);
    }

    public ESLayerReader(SparkSession ss, ESLayerReaderConfig config) {
        super(ss, config.getLayerType());
        this.readerConfig = config;

        this.ss.sparkContext().conf()
                .set("es.nodes", config.getMasterNode())
                .set("es.port", config.getPort())
                .set("es.index.read.missing.as.empty", "true")
                .set("es.nodes.wan.only", "true");
        this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
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
        String source = this.readerConfig.getIndexName() + "/" + this.readerConfig.getTypeName();

        //By JavaEsSpark
        JavaPairRDD<String, Map<String, Object>> rddFromEs = JavaEsSpark.esRDD(this.jsc, source);

        JavaPairRDD<String, Feature> features = rddFromEs.mapToPair(new PairFunction<Tuple2<String, Map<String, Object>>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Map<String, Object>> row) throws Exception {
                String _id = row._1();
                Map<String, Object> _atts = row._2();

                Geometry geometry = null;
                String fid = "";
                Long timestamp = null;
                Long startTime = null;
                Long endTime = null;
                LinkedHashMap<Field, Object> attributes = null;

                //Es自带的pk_id
                fid = _id;

                //set up geometry
                Field geof = readerConfig.getShapeField();
                Map<String, Object> geoShape = (Map<String, Object>) _atts.get(geof.getName());
                String geoType = String.valueOf(geoShape.get("type"));
                String geoCoors = geoShape.get("coordinates").toString();
                String geoJsonTemplate = "{\"geometry\":{\"type\":\"%s\",\"coordinates\":\"%s\"}}";
                GeometryJSON geometryJSON = new GeometryJSON(9);
                geometry = geometryJSON.read(String.format(geoJsonTemplate, geoType, geoCoors));

                //set up timestamp if exists
                Field tf = readerConfig.getTimeField();
                if (tf.getIndex() != Term.FIELD_NOT_EXIST) {
                    timestamp = Long.valueOf(String.valueOf(_atts.get(tf.getName())));
                }

                //set up starttime if exists
                Field stf = readerConfig.getStartTimeField();
                if (stf.isExist()) {
                    startTime = Long.valueOf(String.valueOf(_atts.get(stf.getName())));
                }

                //set up endtime if exists
                Field etf = readerConfig.getEndTimeField();
                if (etf.isExist()) {
                    endTime = Long.valueOf(String.valueOf(_atts.get(etf.getName())));
                }

                //set feature attributes
                attributes = new LinkedHashMap<>();
                Field[] fs = readerConfig.getAttributes();
                for (Field f : fs) {
                    String cname = f.getType();
                    Class c = Class.forName(cname);
                    attributes.put(f, Converter.convert(String.valueOf(_atts.get(tf.getName())), c));
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

    @Override
    public void close() throws IOException {
        ss.close();
        jsc.close();
        log.info(String.format("EsLayerReader close: %s", readerConfig.toString()));
    }

}
