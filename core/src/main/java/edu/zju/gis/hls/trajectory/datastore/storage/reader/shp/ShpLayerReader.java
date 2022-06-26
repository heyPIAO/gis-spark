package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.util.ShpDataReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
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

    private final static String FILE_SPLIT = ";";
    private final static String HEADER_SPLIT = ",";

    public ShpLayerReader(SparkSession ss, ShpLayerReaderConfig config) {
        super(ss, config.getLayerType());
        this.readerConfig = config;
    }

    private void checkReaderConfig() {
        if (this.readerConfig == null) {
            throw new LayerReaderException("set layer reader config");
        }

        if (!this.readerConfig.check()) {
            throw new LayerReaderException("reader config is not set correctly");
        }

    }

    @Override
    public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        JavaRDD<Tuple2<String, Feature>> features = localCommonRead("UTF-8");
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

    public JavaRDD<Tuple2<String, Feature>> localCommonRead(String charset) {
        checkReaderConfig();
        String path = readerConfig.getSourcePath();

        // setup data source
        JavaRDD<String> data = null;
        List<String> paths;
        if (path.contains(FILE_SPLIT)) {
            paths = Arrays.asList(path.split(FILE_SPLIT));
        } else {
            paths = new ArrayList<>();
            paths.add(path);
        }

        //判断路径是否为文件夹，支持二级目录下所有.shp文件路径的获取
        List<String> paths2File = new ArrayList<>();
        for (String subPath : paths) {
            File subFile = new File(subPath.replace(SourceType.SHP.getPrefix(), ""));
            if (subFile.exists()) {
                if (subFile.isFile() && subFile.getName().endsWith(".shp")) {
                    paths2File.add(subFile.getAbsolutePath());
                } else {
                    File[] subSubFiles = subFile.listFiles();
                    for (File subSubFile : subSubFiles) {
                        if (subSubFile.isFile() && subSubFile.getName().endsWith(".shp"))
                            paths2File.add(subSubFile.getAbsolutePath());
                    }
                }
            }
        }

        if (paths2File.size() == 0) {
            throw new GISSparkException("Shapefile Layer Reader Failed, File Path Number Is 0 ");
        }

        data = this.jsc.parallelize(paths2File, paths2File.size());

        JavaRDD<Tuple2<String, Feature>> features = data.flatMap((FlatMapFunction<String, Tuple2<String, Feature>>) s -> {
            List<Tuple2<String, Feature>> result = new ArrayList<>();
            ShpDataReader reader = new ShpDataReader(s);
            reader.init(0, charset);
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

                LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();

                Field[] keys = readerConfig.getAttributes();

                for (Field key : keys) {
                    Object shpField = sf.getAttribute(key.getName());
                    if (shpField != null && shpField.getClass().equals(Date.class)) {
                        Date d = (Date) shpField;
                        java.sql.Date sqlDate = new java.sql.Date(d.getTime());
                        attributes.put(key, sqlDate);
                    } else {
                        attributes.put(key, shpField);
                    }
                }

                // build feature
                Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes);
                result.add(new Tuple2<>(fid, feature));

                sf = reader.nextFeature();
            }
            result.add(new Tuple2<>("PRJ: " + reader.getCrs(), null));
            reader.close();
            return result.iterator();
        });
        return features;
    }

    public T read(String crsStr, String charset) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        JavaRDD<Tuple2<String, Feature>> features = localCommonRead(charset);

        features.cache();

        T layer = this.rddToLayer(features.filter(x -> !x._1.startsWith("PRJ:")).rdd());
        String prjwkt = features.filter(x -> x._1.startsWith("PRJ:")).collect().get(0)._1.substring(4);

        // TODO: I don't really know if it can release the memory
        features.unpersist();

        LayerMetadata lm = new LayerMetadata();
        try {
            CoordinateReferenceSystem crs = null;
            if (crsStr == null || crsStr.trim().equals("")) {
                crs = CRS.parseWKT(prjwkt);
            } else {
                crs = CRS.decode(crsStr);
            }
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

    // 仅支持单文件
    public T readOSS(String crsStr, String charset) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IOException {

        checkReaderConfig();
        String path = readerConfig.getSourcePath();
        // setup data source
        JavaRDD<String> data = null;
        List<String> paths = new ArrayList<>();
        paths.add(path);
        data = this.jsc.parallelize(paths, paths.size());


        JavaRDD<Tuple2<String, Feature>> features = data.flatMap((FlatMapFunction<String, Tuple2<String, Feature>>) s -> {
            List<Tuple2<String, Feature>> result = new ArrayList<>();
            ShpDataReader reader = new ShpDataReader(path);
            reader.init(1, charset);
            String[] headers = reader.getHeaders();
            SimpleFeature sf = reader.nextFeature();

            Field[] keys = readerConfig.getAttributes();
            Field[] fields;
            String[] types;
            int fieldNum;
            if (keys.length == 0) {
                // In this case, all fields should be read
                fieldNum = headers.length;
                fields = new Field[fieldNum];
                types = new String[fieldNum];
                List<Object> attrs = sf.getAttributes();
                for (int i = 0; i < fieldNum; i++) {
                    Object attr = attrs.get(i + 1);
                    types[i] = attr.getClass().getName();
                    fields[i] = new Field(headers[i], types[i]);
                }
            } else {
                // In this case only selected attrs are read
                fieldNum = keys.length;
                fields = new Field[fieldNum];
                types = new String[fieldNum];
                for (int i = 0; i < fieldNum; i++) {
                    Object attr = sf.getAttribute(keys[i].getName());
                    types[i] = attr.getClass().getName();
                    fields[i] = new Field(keys[i].getName(), types[i]);
                }
            }

            readerConfig.setAttributes(fields);
            // TODO check if shp layer type is right to the target layer type
            while (sf != null) {
                Geometry geometry = (Geometry) sf.getDefaultGeometry();
                geometry.setSRID(Integer.parseInt(crsStr.split(":")[1]));

                String fid = UUID.randomUUID().toString();
                Field timeField = readerConfig.getTimeField();

                // 如果有 timestamp，获取
                Long timestamp = null;
                if (timeField.isExist()) {
                    timestamp = Long.valueOf(String.valueOf(sf.getAttribute(timeField.getName())));
                }

                LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();

                // 需要哪些属性
                for (Field key : readerConfig.getAttributes()) {
                    Object shpField = sf.getAttribute(key.getName());
                    if (shpField != null && shpField.getClass().equals(Date.class)) {
                        Date d = (Date) shpField;
                        java.sql.Date sqlDate = new java.sql.Date(d.getTime());
                        attributes.put(key, sqlDate);
                    } else {
                        attributes.put(key, shpField);
                    }
                }

                // build feature
                Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes);
                result.add(new Tuple2<>(fid, feature));

                sf = reader.nextFeature();
            }
            result.add(new Tuple2<>("PRJ: " + reader.getCrs(), null));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fieldNum; i++) {
                if (keys.length == 0) {
                    sb.append(headers[i]);
                } else {
                    sb.append(keys[i].getName());
                }
                sb.append(HEADER_SPLIT).append(types[i]).append(FILE_SPLIT);
            }
            sb.deleteCharAt(sb.length() - 1);
            result.add(new Tuple2<>("FIELDS: " + sb, null));
            reader.close();
            return result.iterator();
        });

        features.cache();

        T layer = this.rddToLayer(features.filter(x -> !x._1.startsWith("PRJ:") && !x._1.startsWith("FIELDS:")).rdd());
        String prjwkt = features.filter(x -> x._1.startsWith("PRJ:")).collect().get(0)._1.substring(4);
        String headerStr = features.filter(x -> x._1.startsWith("FIELDS:")).collect().get(0)._1.substring(8);
        String[] split = headerStr.split(FILE_SPLIT);
        Field[] fields = new Field[split.length];
        for (int i = 0; i < split.length; i++) {
            String[] fieldType = split[i].split(HEADER_SPLIT);
            fields[i] = new Field(fieldType[0], fieldType[1]);
        }
        readerConfig.setAttributes(fields);

        // TODO: I don't really know if it can release the memory
        features.unpersist();

        LayerMetadata lm = new LayerMetadata();
        try {
            CoordinateReferenceSystem crs = CRS.parseWKT(prjwkt);
            lm.setCrs(crs);
            if (!crsStr.equalsIgnoreCase("null")) {
                crs = CRS.decode(crsStr);
                layer.transform(crs);
            }
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
