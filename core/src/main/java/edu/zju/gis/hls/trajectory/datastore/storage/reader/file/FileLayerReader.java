package edu.zju.gis.hls.trajectory.datastore.storage.reader.file;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2019/9/19
 * 从文本文件中读取数据
 * （1）不支持有 header 的文件
 * （2）若不设置空间列，默认最后一列为空间列 wkt
 * （3）若不设置 key(fid) 列，则 key(fid) 会被自动创建（基于UUID）
 * （4）若 T 类型为 TrajectoryPointLayer 则必须设置对应 timestamp
 * 注： headers 中不应有 key(fid)，timestamp, geometry 等字段
 **/
@ToString
@Slf4j
public class FileLayerReader<T extends Layer> extends LayerReader<T> {

    @Getter
    @Setter
    private FileLayerReaderConfig readerConfig;

    public FileLayerReader(SparkSession ss, FileLayerReaderConfig readerConfig) {
        super(ss, readerConfig.getLayerType());
        this.readerConfig = readerConfig;
    }


    @Override
    public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        if (this.readerConfig == null) {
            throw new LayerReaderException("set layer reader config");
        }

        if (!this.readerConfig.check()) {
            throw new LayerReaderException("reader config is not set correctly");
        }

        // set up reader configuration
        String path = readerConfig.getSourcePath();
        CoordinateReferenceSystem crs = readerConfig.getCrs();

        List<String> paths;
        if (path.contains(";")) {
            paths = Arrays.asList(path.split(";"));
        } else {
            paths = new ArrayList<>();
            paths.add(path);
        }

        // 判断路径是否为文件夹，支持二级目录下所有文件路径的获取
        List<String> paths2File = new ArrayList<>();
        for (String subPath : paths) {
            File subFile = new File(subPath.replace(SourceType.FILE.getPrefix(), ""));
            if (subFile.exists()) {
                if (subFile.isFile()) {
                    paths2File.add(subFile.getAbsolutePath());
                } else {
                    File[] subSubFiles = subFile.listFiles();
                    for (File subSubFile : subSubFiles) {
                        if (subSubFile.isFile())
                            paths2File.add(subSubFile.getAbsolutePath());
                    }
                }
            }
        }

        // read data from source files
        JavaRDD<String> data = null;
        String[] ps = new String[paths2File.size()];
        paths2File.toArray(ps);
        data = this.ss.read().textFile(ps).toJavaRDD().filter(x -> x.trim().length() > 0);

        JavaRDD<Tuple2<String, Feature>> features = data.map(new Function<String, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(String s) throws Exception {

                String[] fields = s.split(readerConfig.getSeparator());

                Field[] fs = readerConfig.getAttributes();

                // 获取 fid
                String fid = null;
                if (readerConfig.getIdField().getIndex() == Term.FIELD_NOT_EXIST) {
                    fid = UUID.randomUUID().toString();
                } else {
                    fid = fields[readerConfig.getIdField().getIndex()];
                }

                // 获取 geometry
                String wkt;
                if (readerConfig.getShapeField().getIndex() == Term.FIELD_LAST) {
                    wkt = fields[fields.length - 1];
                } else {
                    wkt = fields[readerConfig.getShapeField().getIndex()];
                }

                WKTReader wktReader = new WKTReader();
                Geometry geometry = wktReader.read(wkt);

                // 如果有 timestamp，获取 timestamp
                Long timestamp = null;
                if (readerConfig.getTimeField().getIndex() != Term.FIELD_NOT_EXIST) {
                    timestamp = Long.valueOf(fields[readerConfig.getTimeField().getIndex()].trim());
                    // 对于TrajectoryPointLayer，geometry需要额外操作
                    if (readerConfig.getLayerType().equals(LayerType.TRAJECTORY_POINT_LAYER)) {
                        if (!(geometry instanceof Point)) {
                            throw new LayerReaderException("TrajectoryPointLayer's geometry type must be point, given " + geometry.getGeometryType());
                        }
                        Point p = (Point) geometry;
                        geometry = new TemporalPoint(timestamp, p.getCoordinateSequence());
                    }
                }


                // 获取 attributes
                LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();


                for (Field f : fs) {
                    int index = f.getIndex();
                    String cname = f.getType();
                    Class c = Class.forName(cname);
                    attributes.put(f, Converter.convert(fields[index], c));
                }

                // build feature
                Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes);
                return new Tuple2<>(fid, feature);
            }
        });

        T layer = this.rddToLayer(features.rdd());

        LayerMetadata lm = new LayerMetadata();
        lm.setLayerId(readerConfig.getLayerId());
        lm.setCrs(crs);
        lm.setLayerName(readerConfig.getLayerName());

        lm.setAttributes(readerConfig.getAllAttributes(false));

        layer.setMetadata(lm);
        return layer;
    }

}
