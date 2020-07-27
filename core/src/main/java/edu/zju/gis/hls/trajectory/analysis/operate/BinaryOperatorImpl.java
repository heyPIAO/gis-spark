package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/7/9
 **/
public abstract class BinaryOperatorImpl implements BinaryOperator {

  protected SparkSession ss;

  protected Boolean attrReserve;

  public BinaryOperatorImpl(SparkSession ss, boolean attrReserve) {
    this.ss = ss;
    this.attrReserve = attrReserve;
  }

  public BinaryOperatorImpl(SparkSession ss) {
    this(ss, false);
  }

  @Override
  public Layer run(Feature f, Layer layer) {
    List<Feature> fs = new ArrayList<>();
    fs.add(f);
    return this.run(fs, layer);
  }

  @Override
  public Layer run(Geometry g, Layer layer) {
    if (!(g instanceof org.locationtech.jts.geom.Polygon || g instanceof org.locationtech.jts.geom.MultiPolygon)) {
      throw new GISSparkException("Unvalid operator for geometry type: " + g.toString());
    }
    Feature f = Feature.buildFeature(UUID.randomUUID().toString(), g, new LinkedHashMap<>());
    List<Feature> fs = new ArrayList<>();
    fs.add(f);
    return this.run(fs, layer);
  }

  @Override
  public Layer run(Feature f, IndexedLayer layer) {
    List<Feature> fs = new ArrayList<>();
    fs.add(f);
    return this.run(fs, layer);
  }

  @Override
  public Layer run(Geometry f, IndexedLayer layer) {
    return this.run(f, layer.query(f));
  }

  @Override
  public Layer run(IndexedLayer layer1, IndexedLayer layer2) {
    throw new GISSparkException("Under developing");
  }

  @Override
  public Layer run(Layer layer1, IndexedLayer layer2) {
    throw new GISSparkException("Under developing");
  }

  @Override
  public Layer run(List<Feature> features, IndexedLayer layer) {
    return this.run(features,
      layer.query(features.stream().map((java.util.function.Function<Feature, Geometry>) Feature::getGeometry)
      .collect(Collectors.toList())));
  }
}
