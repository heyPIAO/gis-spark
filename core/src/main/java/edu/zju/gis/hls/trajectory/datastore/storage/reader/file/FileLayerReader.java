package edu.zju.gis.hls.trajectory.datastore.storage.reader.file;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2019/9/19
 * 从文本文件中读取数据
 * （1）不支持有 header 的文件
 * （2）若不设置空间列，默认最后一列为空间列 wkt
 * （3）若不设置 key(fid) 列，则 key(fid) 会被自动创建（基于UUID）
 * （4）若 T 类型为 TrajectoryPointLayer 或 TrajectoryPolylineLayer，则必须设置对应 timestamp 或者是 startTime, endTime 字段
 * 注： headers 中不应有 key(fid)，timestamp, startTime, endTime，geometry 等字段
 **/
@ToString
public class FileLayerReader <T extends Layer> extends LayerReader<T> {

  private static final Logger logger = LoggerFactory.getLogger(FileLayerReader.class);

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

    // read data from source files
    JavaRDD<String> data = null;
    if (path.contains(";")){
      String[] paths = path.split(";");
      data = this.ss.read().textFile(paths).toJavaRDD().filter(x->x.trim().length()>0);
    } else {
      data = this.ss.read().textFile(path).toJavaRDD().filter(x->x.trim().length()>0);
    }

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
        }

        // 如果有 startTime，endTime，获取
        Long startTime = null;
        if (readerConfig.getStartTimeField().getIndex() != Term.FIELD_NOT_EXIST) {
          startTime = Long.valueOf(fields[readerConfig.getStartTimeField().getIndex()].trim());
        }

        Long endTime = null;
        if (readerConfig.getEndTimeField().getIndex() != Term.FIELD_NOT_EXIST) {
          endTime = Long.valueOf(fields[readerConfig.getEndTimeField().getIndex()].trim());
        }

        // 获取 attributes
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();

        for (Field f: fs) {
          int index = f.getIndex();
          String cname = f.getType();
          Class c = Class.forName(cname);
          attributes.put(f, Converter.convert(fields[index], c));
        }

        // build feature
        Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes, timestamp, startTime, endTime);
        return new Tuple2<> (fid, feature);
      }
    });

    T layer = this.rddToLayer(features.rdd());

    LayerMetadata lm = new LayerMetadata();
    lm.setLayerId(readerConfig.getLayerId());
    lm.setCrs(crs);
    lm.setLayerName(readerConfig.getLayerName());
    lm.setAttributes(readerConfig.getAllAttributes());

    layer.setMetadata(lm);
    return layer;
  }

  @Override
  public void close() throws IOException {
    logger.info("close file reader " + this.toString());
  }

}
