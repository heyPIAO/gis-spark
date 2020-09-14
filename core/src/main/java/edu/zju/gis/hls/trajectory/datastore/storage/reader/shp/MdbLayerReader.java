package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.util.MdbDataReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2020/9/14
 * 目前仅支持：Double，Integer，String
 **/
@Slf4j
public class MdbLayerReader<T extends Layer> extends LayerReader<T> {

  @Getter
  @Setter
  private MdbLayerReaderConfig readerConfig;

  public MdbLayerReader(SparkSession ss, MdbLayerReaderConfig config) {
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
    if (path.contains(";")) {
      paths = Arrays.asList(path.split(";"));
    } else {
      paths = new ArrayList<>();
      paths.add(path);
    }

    //判断路径是否为文件夹，支持二级目录下所有.mdb文件路径的获取
    List<String> paths2File = new ArrayList<>();
    for (String subPath : paths) {
      File subFile = new File(subPath.replace(SourceType.MDB.getPrefix(),""));
      if (subFile.exists()) {
        if (subFile.isFile() && subFile.getName().endsWith(".mdb")) {
          paths2File.add(subFile.getAbsolutePath());
        } else {
          File[] subSubFiles = subFile.listFiles();
          for (File subSubFile : subSubFiles) {
            if (subSubFile.isFile() && subSubFile.getName().endsWith(".mdb"))
              paths2File.add(subSubFile.getAbsolutePath());
          }
        }
      }
    }

    data = this.jsc.parallelize(paths2File, paths2File.size());

    JavaRDD<Tuple2<String, Feature>> features = data.flatMap(new FlatMapFunction<String, Tuple2<String, Feature>>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(String s) throws Exception {
        List<Tuple2<String, Feature>> result = new ArrayList<>();
        MdbDataReader reader = new MdbDataReader(s, readerConfig.getTargetLayerName());
        reader.init();
        org.gdal.ogr.Feature sf = reader.nextFeature();
        WKTReader2 wktReader = new WKTReader2();
        // TODO check if shp layer type is right to the target layer type
        while (sf != null) {
          Geometry geometry = wktReader.read(sf.GetGeometryRef().ExportToWkt());

          String fid = UUID.randomUUID().toString();
          Field timeField = readerConfig.getTimeField();

          // 如果有 timestamp，获取
          Long timestamp = null;
          if (timeField.isExist()) {
            timestamp = Long.valueOf(sf.GetFieldAsString(timeField.getName()));
          }

          // 如果有 startTime/endTime，获取
          Field startTimeField = readerConfig.getStartTimeField();
          Long startTime = null;
          if (startTimeField.isExist()) {
            startTime = Long.valueOf(sf.GetFieldAsString(startTimeField.getName()));
          }

          Field endTimeField = readerConfig.getEndTimeField();
          Long endTime = null;
          if (endTimeField.isExist()) {
            endTime = Long.valueOf(sf.GetFieldAsString(endTimeField.getName()));
          }

          LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();

          Field[] keys = readerConfig.getAttributes();

          for (Field key : keys) {
            attributes.put(key, MdbDataReader.getField(sf, key.getName()));
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
