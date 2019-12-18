package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Closeable;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static edu.zju.gis.hls.trajectory.datastore.storage.config.ReaderConfig.*;


/**
 * @author Hu
 * @date 2019/9/19
 * 分布式环境下数据读取基类
 * 原理：基于 SparkSession 对于各类datasource的读取API封装，以支持更多业务操作
 **/
public abstract class LayerReader<T extends Layer> implements Closeable, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(LayerReader.class);

  @Getter
  transient protected SparkSession ss;

  @Getter
  transient protected JavaSparkContext jsc;

  @Getter
  protected Class featureType;

  @Getter
  @Setter
  protected Properties prop; // 数据源配置

  public LayerReader(SparkSession ss, Class featureType) {
    this.ss = ss;
    this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
    this.featureType = featureType;
  }

  public abstract T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

  /**
   * 如果是trajectory图层，检查图层配置参数
   * @param featureType
   * @return
   */
  protected boolean checkTrajectoryConfig(FeatureType featureType) {
    if (featureType.equals(FeatureType.TRAJECTORY_POLYLINE)){
      Object startTime = this.prop.getProperty(START_TIME_INDEX);
      Object endTime = this.prop.getProperty(END_TIME_INDEX);
      if (startTime != null && endTime != null) {
        return true;
      } else {
        logger.error("trajectory polyline layer needs to specify start time field index and end time field index");
      }
    } else if (featureType.equals(FeatureType.TRAJECTORY_POINT)) {
      Object timestamp = this.prop.getProperty(TIME_INDEX);
      if (timestamp != null) {
        return true;
      } else {
        logger.error("trajectory point layer needs to specify timestamp field index");
      }
    } else {
      logger.error("unsupport trajectory geometry type");
    }
    return false;
  }

  protected Class<T> getTClass() {
    FeatureType featureType = FeatureType.getType(this.featureType.getName());
    LayerType layerType = featureType.getLayerType();
    try {
      return (Class<T>) Class.forName(layerType.getClassName());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }

  protected T rddToLayer(RDD<Tuple2<String, Feature>> features) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Class<T> resultClass = this.getTClass();
    Constructor resultClassConstructor = resultClass.getConstructor(RDD.class);
    T result = (T) resultClassConstructor.newInstance(features);
    return result;
  }

  protected Feature buildFeature(FeatureType featureType, String fid, Geometry geometry, Map<String, Object> attributes, Long timestamp, Long startTime, Long endTime) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    // 基于 java reflect 实现动态类 Feature 构造
    String className = featureType.getClassName();
    Class featureClass = Class.forName(className);
    Object feature;
    Constructor c;

    // 根据不同的geometryType获取对应的构造函数并获取对应实例
    if (featureType.equals(FeatureType.POINT)) {
      c = featureClass.getConstructor(String.class, Point.class, Map.class);
      feature = c.newInstance(fid, (Point)geometry, attributes);
    } else if (featureType.equals(FeatureType.POLYLINE)) {
      c = featureClass.getConstructor(String.class, LineString.class, Map.class);
      feature = c.newInstance(fid, (LineString)geometry, attributes);
    } else if (featureType.equals(FeatureType.POLYGON)) {
      c = featureClass.getConstructor(String.class, Polygon.class, Map.class);
      feature = c.newInstance(fid, (Polygon)geometry, attributes);
    } else if (featureType.equals(FeatureType.TRAJECTORY_POINT)) {
      c = featureClass.getConstructor(String.class, Point.class, Map.class, long.class);
      feature = c.newInstance(fid, (Point)geometry, attributes, timestamp.longValue());
    } else if (featureType.equals(FeatureType.TRAJECTORY_POLYLINE)) {
      c = featureClass.getConstructor(String.class, LineString.class, Map.class, long.class, long.class);
      feature = c.newInstance(fid, (LineString)geometry, attributes, startTime.longValue(), endTime.longValue());
    } else {
      logger.error("Unsupport feature type: " + featureType.getName());
      return null;
    }
    return (Feature) feature;
  }

  private List<Field> getTFields() {
    List<Field> fields = new ArrayList<>();
    Class <T> c = getTClass();
    // 迭代获取类及父类中的所有字段
    while(c!=null){
      fields.addAll(Arrays.asList(c.getDeclaredFields()));
      c = (Class <T>)c.getSuperclass();
    }
    return fields;
  }

}
