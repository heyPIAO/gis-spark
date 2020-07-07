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
import org.locationtech.jts.geom.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;


/**
 * @author Hu
 * @date 2019/9/19
 * 分布式环境下数据读取基类
 * 原理：基于 SparkSession 对于各类 datasource 的读取API封装，以支持更多业务操作
 **/
public abstract class LayerReader<T extends Layer> implements Closeable, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(LayerReader.class);

  @Getter
  transient protected SparkSession ss;

  @Getter
  transient protected JavaSparkContext jsc;

  @Setter
  protected LayerType layerType;

  public LayerReader(SparkSession ss, LayerType layerType) {
    this.ss = ss;
    this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
    this.layerType = layerType;
  }

  public abstract T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

  /**
   * 获取图层类型对应的类
   * @return
   */
  protected Class<T> getTClass() {
    return (Class<T>) this.layerType.getLayerClass();
  }

  protected T rddToLayer(RDD<Tuple2<String, Feature>> features) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Class<T> resultClass = this.getTClass();
    Constructor resultClassConstructor = resultClass.getConstructor(RDD.class);
    T result = (T) resultClassConstructor.newInstance(features);
    return result;
  }

  protected Feature buildFeature(FeatureType featureType, String fid, Geometry geometry, LinkedHashMap<edu.zju.gis.hls.trajectory.analysis.model.Field, Object> attributes, Long timestamp, Long startTime, Long endTime) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    // 基于 java reflect 实现动态类 Feature 构造
    String className = featureType.getClassName();
    Class featureClass = Class.forName(className);
    Object feature;
    Constructor c;

    // 根据不同的 geometryType 获取对应的构造函数并获取对应实例
    if (featureType.equals(FeatureType.POINT)) {
      c = featureClass.getConstructor(String.class, Point.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (Point)geometry, attributes);
    } else if (featureType.equals(FeatureType.POLYLINE)) {
      c = featureClass.getConstructor(String.class, LineString.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (LineString)geometry, attributes);
    } else if (featureType.equals(FeatureType.POLYGON)) {
      c = featureClass.getConstructor(String.class, Polygon.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (Polygon)geometry, attributes);
    } else if (featureType.equals(FeatureType.MULTI_POINT)) {
      c = featureClass.getConstructor(String.class, MultiPoint.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (MultiPoint)transformToMulti(geometry), attributes);
    } else if (featureType.equals(FeatureType.MULTI_POLYLINE)) {
      c = featureClass.getConstructor(String.class, MultiLineString.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (MultiLineString)transformToMulti(geometry), attributes);
    } else if (featureType.equals(FeatureType.MULTI_POLYGON)) {
      c = featureClass.getConstructor(String.class, MultiPolygon.class, LinkedHashMap.class);
      feature = c.newInstance(fid, (MultiPolygon)transformToMulti(geometry), attributes);
    } else if (featureType.equals(FeatureType.TRAJECTORY_POINT)) {
      c = featureClass.getConstructor(String.class, Point.class, LinkedHashMap.class, long.class);
      feature = c.newInstance(fid, (Point)geometry, attributes, timestamp.longValue());
    } else if (featureType.equals(FeatureType.TRAJECTORY_POLYLINE)) {
      c = featureClass.getConstructor(String.class, LineString.class, LinkedHashMap.class, long.class, long.class);
      feature = c.newInstance(fid, (LineString)geometry, attributes, startTime.longValue(), endTime.longValue());
    } else {
      logger.error("Unsupport feature type: " + featureType.getName());
      return null;
    }
    return (Feature) feature;
  }

  private List<Field> getTFields() {
    List<Field> fields = new ArrayList<>();
    Class<T> c = getTClass();
    // 迭代获取类及父类中的所有字段
    while(c!=null){
      fields.addAll(Arrays.asList(c.getDeclaredFields()));
      c = (Class <T>)c.getSuperclass();
    }
    return fields;
  }

  /**
   * 统一 Multi 图层的 Geometry 类型
   * @param geometry
   * @return
   */
  private Geometry transformToMulti(Geometry geometry) {
    GeometryFactory gf = new GeometryFactory();
    if (geometry instanceof Point) {
      Point[] ps = new Point[1];
      ps[0] = (Point) geometry;
      return gf.createMultiPoint(ps);
    } else if (geometry instanceof LineString) {
      LineString[] ls = new LineString[1];
      ls[0] = (LineString) geometry;
      return gf.createMultiLineString(ls);
    } else if (geometry instanceof Polygon) {
      Polygon[] pls = new Polygon[1];
      pls[0] = (Polygon) geometry;
      return gf.createMultiPolygon(pls);
    } else {
      return geometry;
    }
  }

  @Override
  public void close() throws IOException {

  }

}
