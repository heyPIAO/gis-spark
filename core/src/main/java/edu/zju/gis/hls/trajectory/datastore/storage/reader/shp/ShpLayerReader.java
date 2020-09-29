package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.util.PathSelector;
import edu.zju.gis.hls.trajectory.datastore.util.ShpDataReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;


/**
 * @author Hu
 * @date 2019/11/12
 * 从 shapefile 文件中读取数据，并转为 Layer
 * 各个文件路径以“;”分隔
 * TODO 暂时不支持存储在HDFS中的shapefile文件
 **/
@Slf4j
public class ShpLayerReader<T extends Layer> extends LayerReader<T> {

    @Getter
    @Setter
    private ShpLayerReaderConfig readerConfig;

    public ShpLayerReader(SparkSession ss, ShpLayerReaderConfig config) {
        super(ss, config.getLayerType());
        this.readerConfig = config;
    }

    @Override
    public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        if (this.readerConfig == null) {
            throw new LayerReaderException("set layer reader config");
        }

        if (!this.readerConfig.check()) {
            throw new LayerReaderException("reader config is not set correctly");
        }

        //判断路径是否为文件夹，支持二级目录下所有.shp文件路径的获取
        List<String> paths2File = PathSelector.stripPath(readerConfig.getSourcePath(),SourceType.SHP.getPrefix(),".shp");

        if (paths2File.size() == 0) {
            throw new GISSparkException("Shapefile Layer Reader Failed, File Path Number Is 0 ");
        }
        JavaRDD<String> data = null;
        data = this.jsc.parallelize(paths2File, paths2File.size());

        JavaRDD<Tuple2<String, Feature>> features = data.flatMap(new FlatMapFunction<String, Tuple2<String, Feature>>() {
            @Override
            public Iterator<Tuple2<String, Feature>> call(String s) throws Exception {
                List<Tuple2<String, Feature>> result = new ArrayList<>();
                ShpDataReader reader = new ShpDataReader(s);
                reader.init();
                SimpleFeature sf = reader.nextFeature();
                // TODO check if shp layer type is right to the target layer type
                while (sf != null) {
                    Geometry geometry = (Geometry) sf.getDefaultGeometry();

                    String fid = UUID.randomUUID().toString();
                    Field timeField = readerConfig.getTimeField();

                    // 如果有 timestamp，获取
                    Long timestamp = null;
                    if (timeField.isExist()) {
                        timestamp = Long.valueOf(String.valueOf(sf.getAttribute(timeField.getName())));
                    }

                    // 如果有 startTime/endTime，获取
                    Field startTimeField = readerConfig.getStartTimeField();
                    Long startTime = null;
                    if (startTimeField.isExist()) {
                        startTime = Long.valueOf(String.valueOf(sf.getAttribute(startTimeField.getName())));
                    }

                    Field endTimeField = readerConfig.getEndTimeField();
                    Long endTime = null;
                    if (endTimeField.isExist()) {
                        endTime = Long.valueOf(String.valueOf(sf.getAttribute(endTimeField.getName())));
                    }

                    LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();

                    Field[] keys = readerConfig.getAttributes();

                    for (Field key : keys) {
                        Object shpField = sf.getAttribute(key.getName());
                        if (shpField != null && shpField.getClass().equals(java.util.Date.class)) {
                            Date d = (java.util.Date) shpField;
                            java.sql.Date sqlDate = new java.sql.Date(d.getTime());
                            attributes.put(key, sqlDate);
                        } else {
                            attributes.put(key, shpField);
                        }
                    }

                    // build feature
                    Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes, timestamp, startTime, endTime);
                    result.add(new Tuple2<>(fid, feature));

                    sf = reader.nextFeature();
                }
                result.add(new Tuple2<>("PRJ: " + reader.getCrs(), null));
                reader.close();
                return result.iterator();
            }
        });

        features.cache();

        T layer = this.rddToLayer(features.filter(x -> !x._1.startsWith("PRJ:")).rdd());
        String prjwkt = features.filter(x -> x._1.startsWith("PRJ:")).collect().get(0)._1.substring(4);

        // TODO: I don't really know if it can release the memory
        features.unpersist();

        LayerMetadata lm = new LayerMetadata();
        try {
            CoordinateReferenceSystem crs = CRS.parseWKT(prjwkt);
            lm.setCrs(crs);
        } catch (FactoryException e) {
            log.error(e.getMessage());
            log.warn("Set coordinate reference default to wgs84");
        }

        lm.setLayerId(readerConfig.getLayerId());
        lm.setLayerName(readerConfig.getLayerName());

        // this.readerConfig.getIdField().setIndex(Term.FIELD_EXIST);
        lm.setAttributes(readerConfig.getAllAttributes());

        layer.setMetadata(lm);
        return layer;
    }

}
