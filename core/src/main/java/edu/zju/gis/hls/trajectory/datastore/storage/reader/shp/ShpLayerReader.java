package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.DataReaderException;
import edu.zju.gis.hls.trajectory.datastore.util.ShpDataReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm.*;
import static edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm.ATTRIBUTE_TYPE;
import static edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm.HEADER_INDEX;

/**
 * @author Hu
 * @date 2019/11/12
 * 从 shapefile 文件中读取数据，并转为为 Layer
 * 各个文件路径以“;”分隔
 * TODO 暂时不支持存储在HDFS中的shapefile文件
 **/
public class ShpLayerReader <T extends Layer> extends LayerReader <T> {

  private static final Logger logger = LoggerFactory.getLogger(ShpLayerReader.class);

  public ShpLayerReader(SparkSession ss, Class featureType) {
    super(ss, featureType);
  }

  @Override
  public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    Object obj = this.prop.getProperty(FILE_PATH);
    if (obj == null) {
      logger.error("has not set file path yet");
      throw new DataReaderException("has not set file path yet");
    }

    // configure shape type
    FeatureType featureType = FeatureType.getType(this.featureType.getName());
    if (featureType.getName().toLowerCase().contains("trajectory")) {
      boolean check = checkTrajectoryConfig(featureType);
      if (!check) {
        throw new DataReaderException("unvalid configuration for data reader");
      }
    }

    String path = (String) obj;

    // setup data source
    JavaRDD<String> data = null;
    List<String> paths;
    if (path.contains(";")){
      paths = Arrays.asList(path.split(";"));
    } else {
      paths = new ArrayList<>();
      paths.add(path);
    }
    data = this.jsc.parallelize(paths, paths.size());

    // if trajectory point feature, needs to set timestamp field
    // TODO 暂时只支持 ms 时间戳
    String timeIndex = null;
    if (featureType.equals(FeatureType.TRAJECTORY_POINT)) {
      timeIndex = this.prop.getProperty(TIME_INDEX);
    }

    // if trajectory polyline feature, needs to set start time field and end time field
    String startTimeIndex = null;
    String endTimeIndex = null;
    if (featureType.equals(FeatureType.TRAJECTORY_POLYLINE)) {
      startTimeIndex = this.prop.getProperty(START_TIME_INDEX);
      endTimeIndex = this.prop.getProperty(END_TIME_INDEX);
    }

    // set up attributes
    Gson gson = new Gson();
    String headerJson = this.prop.getProperty(HEADER_INDEX, gson.toJson(new HashMap<String, String>()));
    HashMap<String, String> headers = gson.fromJson(headerJson, new HashMap<String, String>().getClass());

    // set up attribute types
    String attributeTypeJson = this.prop.getProperty(ATTRIBUTE_TYPE, gson.toJson(new HashMap<String, String>()));
    HashMap<String, String> attributeType = gson.fromJson(attributeTypeJson, new  HashMap<String, String>().getClass());

    final Broadcast<FeatureType> geometryTypeBroad = this.jsc.broadcast(featureType);
    final Broadcast<String> timeIndexBroad = this.jsc.broadcast(timeIndex);
    final Broadcast<String> startTimeIndexBroad = this.jsc.broadcast(startTimeIndex);
    final Broadcast<String> endTimeIndexBroad = this.jsc.broadcast(endTimeIndex);
    final Broadcast<HashMap<String, String>> headersBroad = this.jsc.broadcast(headers);

    JavaRDD<Tuple2<String, Feature>> features = data.flatMap(new FlatMapFunction<String, Tuple2<String, Feature>>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(String s) throws Exception {
        List<Tuple2<String, Feature>> result = new ArrayList<>();
        ShpDataReader reader = new ShpDataReader(s);
        reader.init();
        List<String> headers = Arrays.asList(reader.getHeaders());
        SimpleFeature sf = reader.nextFeature();
        // TODO check if shp layer type is right to the target layer type
        while (sf != null) {
          Geometry geometry = (Geometry) sf.getDefaultGeometry();
          String fid = sf.getID();

          // 如果有 timestamp，获取 timestamp
          Long timestamp = null;
          if (timeIndexBroad.getValue() != null) {
            timestamp = Long.valueOf(String.valueOf(sf.getAttribute(timeIndexBroad.getValue())));
          }

          // 如果有 startTime，endTime，获取
          Long startTime = null;
          if (startTimeIndexBroad.getValue() != null) {
            startTime = Long.valueOf(String.valueOf(sf.getAttribute(startTimeIndexBroad.getValue())));
          }

          Long endTime = null;
          if (endTimeIndexBroad.getValue() != null) {
            endTime = Long.valueOf(String.valueOf(sf.getAttribute(endTimeIndexBroad.getValue())));
          }

          Map<String, Object> attributes = new HashMap<>();

          Set<String> keys = headersBroad.getValue().keySet();
          for (String key: headersBroad.getValue().keySet()) {
            String index = headersBroad.getValue().get(key);
            if (!headers.contains(index)) {
              logger.warn(String.format("Header %s not exist in file %s, abort it", index, s));
              keys.remove(key);
            }
          }

          if (headersBroad.getValue() != null && headersBroad.getValue().size() > 0) {
            // 获取指定字段信息
            for (String key: keys) {
              String index = headersBroad.getValue().get(key);
              attributes.put(key, sf.getAttribute(index));
            }
          } else {
            // 获取所有字段信息
            for (String key: headers) {
              attributes.put(key, sf.getAttribute(key));
            }
          }
          // build feature
          Feature feature = buildFeature(geometryTypeBroad.getValue(), fid, geometry, attributes, timestamp, startTime, endTime);
          result.add(new Tuple2<>(fid, feature));

          sf = reader.nextFeature();
        }
        result.add(new Tuple2<>("PRJ: " + reader.getCrs(), null));
        return result.iterator();
      }
    });

    features.cache();

    T layer = this.rddToLayer(features.filter(x->!x._1.startsWith("PRJ:")).rdd());
    String prjwkt = features.filter(x->x._1.startsWith("PRJ:")).collect().get(0)._1.substring(4);

    // TODO: I don't really know if it can release the memory
    features.unpersist();

    LayerMetadata lm = new LayerMetadata();
    try {
      CoordinateReferenceSystem crs = CRS.decode(prjwkt);
      lm.setCrs(crs);
    } catch (FactoryException e) {
      logger.error(e.getMessage());
      logger.warn("Set coordinate reference default to wgs84");
    }

    lm.setLayerId(UUID.randomUUID().toString());
    lm.setLayerName(this.prop.getProperty(ReaderConfigTerm.LAYER_NAME, lm.getLayerId()));

    layer.setAttributeTypes(attributeType);
    layer.setMetadata(lm);
    return layer;
  }

  @Override
  public void close() throws IOException {

  }

}
