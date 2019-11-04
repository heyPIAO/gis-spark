package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig;
import lombok.ToString;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig.*;

/**
 * @author Hu
 * @date 2019/9/19
 * 从文本文件中读取数据
 * （1）不支持有 header 的文件
 * （2）若不设置空间列，默认最后一列为空间列 wkt
 * （3）若不设置 fid 列，则 fid 会被自动创建（基于UUID）
 * （4）若 T 类型为 TrajectoryPointLayer 或 TrajectoryPolylineLayer，则必须设置对应 timestamp 或者是 startTime, endTime 字段
 * 注： headers 中不应有 fid，timestamp, startTime, endTime，geometry 等字段
 **/
@ToString
public class FileLayerReader <T extends Layer> extends LayerReader <T> {

  private static final Logger logger = LoggerFactory.getLogger(FileLayerReader.class);

  public FileLayerReader(SparkSession ss, Class featureType) {
    super(ss, featureType);
  }

  @Override
  public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    Object obj = this.prop.getProperty(FILE_PATH);

    if (obj == null) {
      logger.error("has not set file path yet");
      return null;
    }

    // configure shape type
    FeatureType featureType = FeatureType.getType(this.featureType.getName());
    if (featureType.getName().toLowerCase().contains("trajectory")) {
      boolean check = checkTrajectoryConfig(featureType);
      if (!check) {
        return null;
      }
    }

    String path = (String)obj;

    // setup data source
    JavaRDD<String> data = null;
    if (path.contains(";")){
      String[] paths = path.split(";");
      data = this.ss.read().textFile(paths).toJavaRDD();
    } else {
      data = this.ss.read().textFile(path).toJavaRDD();
    }

    // configure fid index
    // 若不设置空间列，默认最后一列为wkt空间列
    Integer shapeIndex = Integer.valueOf(this.prop.getProperty(SHAPE_INDEX, "-1"));
    if (shapeIndex == null) {
      logger.warn("shape index set error, default set to the last column");
      shapeIndex = -1;
    }

    // configure shape index
    // 若不设置空间列，默认最后一列为wkt空间列
    Integer fidIndex = Integer.valueOf(this.prop.getProperty(SHAPE_INDEX, "-99"));
    if (fidIndex == null || fidIndex == -99) {
      logger.warn("fid index not set or fid index set error, default set based on UUID()");
      fidIndex = -99;
    }

    // configure separator
    String separator = this.prop.getProperty(SEPARATOR, "\t");
    if (separator.trim().length() == 0) {
      logger.warn("field separator set error, default set to tab");
      separator = "\t";
    }

    // if trajectory point feature, needs to set timestamp field
    // TODO 暂时只支持 ms 时间戳
    Integer timeIndex = -99;
    if (featureType.equals(FeatureType.TRAJECTORY_POINT)) {
      timeIndex = Integer.valueOf(this.prop.getProperty(TIME_INDEX));
    }

    // if trajectory polyline feature, needs to set start time field and end time field
    Integer startTimeIndex = -99;
    Integer endTimeIndex = -99;
    if (featureType.equals(FeatureType.TRAJECTORY_POLYLINE)) {
      startTimeIndex = Integer.valueOf(this.prop.getProperty(START_TIME_INDEX));
      endTimeIndex = Integer.valueOf(this.prop.getProperty(END_TIME_INDEX));
    }

    // set up attributes
    Gson gson = new Gson();
    String headerJson = this.prop.getProperty(HEADER_INDEX, gson.toJson(new HashMap<String, String>()));
    HashMap<String, String> headers = gson.fromJson(headerJson, new HashMap<String, String>().getClass());

    // set up attribtue types
    String attributeTypeJson = this.prop.getProperty(ATTRIBUTE_TYPE, gson.toJson(new HashMap<String, String>()));
    HashMap<String, String> attributeType = gson.fromJson(attributeTypeJson, new  HashMap<String, String>().getClass());

    final Broadcast<String> separatorBroad =this.jsc.broadcast(separator);
    final Broadcast<Integer> shapeIndexBroad = this.jsc.broadcast(shapeIndex);
    final Broadcast<FeatureType> geometryTypeBroad = this.jsc.broadcast(featureType);
    final Broadcast<Integer> timeIndexBroad = this.jsc.broadcast(timeIndex);
    final Broadcast<Integer> startTimeIndexBroad = this.jsc.broadcast(startTimeIndex);
    final Broadcast<Integer> endTimeIndexBroad = this.jsc.broadcast(endTimeIndex);
    final Broadcast<Integer> fidIndexBroad = this.jsc.broadcast(fidIndex);
    final Broadcast<HashMap<String, String>> headersBroad = this.jsc.broadcast(headers);
    final Broadcast<HashMap<String, String>> attributeTypeBroad = this.jsc.broadcast(attributeType);

    JavaRDD<Tuple2<String, Feature>> features = data.map(new Function<String, Tuple2<String, Feature>>() {
      @Override
      public Tuple2<String, Feature> call(String s) throws Exception {
        String[] fields = s.split(separatorBroad.getValue());

        // 获取 fid
        String fid = null;
        if (fidIndexBroad.getValue() == -99) {
          fid = UUID.randomUUID().toString();
        } else {
          fid = fields[fidIndexBroad.getValue()];
        }

        // 获取 geometry
        String wkt;
        if (shapeIndexBroad.getValue() == -1) {
          wkt = fields[fields.length - 1];
        } else {
          wkt = fields[shapeIndexBroad.getValue()];
        }

        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(wkt);

        // 如果有 timestamp，获取 timestamp
        Long timestamp = null;
        if (timeIndexBroad.getValue() != -99) {
          timestamp = Long.valueOf(fields[timeIndexBroad.getValue()].trim());
        }

        // 如果有 startTime，endTime，获取
        Long startTime = null;
        if (startTimeIndexBroad.getValue() != -99) {
          startTime = Long.valueOf(fields[startTimeIndexBroad.getValue()].trim());
        }

        Long endTime = null;
        if (endTimeIndexBroad.getValue() != -99) {
          endTime = Long.valueOf(fields[endTimeIndexBroad.getValue()].trim());
        }

        // 获取 attributes
        Map<String, Object> attributes = new HashMap<>();
        Map<String, String> attributesType = attributeTypeBroad.getValue();
        for (String key: headersBroad.getValue().keySet()) {
          int index = Integer.valueOf(headersBroad.getValue().get(key));
          String cname = attributesType.get(key);
          if (cname == null) {
            cname = String.class.getName();
          }
          Class c = Class.forName(cname);
          attributes.put(key, Converter.convert(fields[index], c));
        }

        // build feature
        Feature feature = buildFeature(geometryTypeBroad.getValue(), fid, geometry, attributes, timestamp, startTime, endTime);
        return new Tuple2<> (fid, feature);
      }
    });

    T layer = this.rddToLayer(features.rdd());
    LayerMetadata lm = new LayerMetadata();
    lm.setLayerId(UUID.randomUUID().toString());
    lm.setLayerName(this.prop.getProperty(ReaderConfig.LAYER_NAME, lm.getLayerId()));
    layer.setAttributeTypes(attributeType);
    layer.setMetadata(lm);

    return layer;
  }

  @Override
  public void close() throws IOException {
    logger.info("close file reader " + this.toString());
  }

}
