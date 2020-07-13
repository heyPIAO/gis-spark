package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.util.ShpDataReader;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.*;


/**
 * @author Hu
 * @date 2019/11/12
 * 从 shapefile 文件中读取数据，并转为 Layer
 * 各个文件路径以“;”分隔
 * TODO 暂时不支持存储在HDFS中的shapefile文件
 **/
public class ShpLayerReader <T extends Layer> extends LayerReader<T> {

  private static final Logger logger = LoggerFactory.getLogger(ShpLayerReader.class);

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

    String path = readerConfig.getSourcePath();

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

          String fid = sf.getID();
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

          for (Field key: keys) {
            attributes.put(key, sf.getAttribute(key.getName()));
          }

          // build feature
          Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes, timestamp, startTime, endTime);
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
      CoordinateReferenceSystem crs = CRS.parseWKT(prjwkt);
      lm.setCrs(crs);
    } catch (FactoryException e) {
      logger.error(e.getMessage());
      logger.warn("Set coordinate reference default to wgs84");
    }

    lm.setLayerId(readerConfig.getLayerId());
    lm.setLayerName(readerConfig.getLayerName());

    // this.readerConfig.getIdField().setIndex(Term.FIELD_EXIST);
    lm.setAttributes(readerConfig.getAllAttributes());

    layer.setMetadata(lm);
    return layer;
  }

}
