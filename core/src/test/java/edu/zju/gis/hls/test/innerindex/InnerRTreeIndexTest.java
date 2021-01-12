package edu.zju.gis.hls.test.innerindex;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.InnerSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.uniformGrid.UniformGridConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;

import java.util.List;


/**
 * @author Hu
 * @date 2021/1/4
 **/
public class InnerRTreeIndexTest {

  public static void main(String[] args) throws Exception {

    String queryWkt = "Polygon ((117.38500701 31.40472141, 117.41999073 29.25905273, 120.21868901 29.24739148, 120.21868901 31.32309271, 120.21868901 31.32309271, 117.38500701 31.40472141))";
    WKTReader reader = new WKTReader();
    Geometry QueryGeometry = reader.read(queryWkt);

    SparkSession ss = SparkSession.builder()
      .master("local[4]")
      .appName("InnerRTreeIndexTest")
      .getOrCreate();

    Field shapeField = Term.FIELD_DEFAULT_SHAPE;
    shapeField.setIndex(0);
    Field fid = new Field("FID","FeatureID", 1, FieldType.ID_FIELD);

    String dataFile = "file:///D:\\Work\\DaLunWen\\code\\trajectory-spark\\core\\src\\test\\resources\\Province.csv";

    LayerReaderConfig config = new FileLayerReaderConfig();
    config.setLayerId("Province_uniform");
    config.setLayerName("Province_uniform");
    config.setCrs(Term.DEFAULT_CRS);
    config.setSourcePath(dataFile);
    config.setLayerType(LayerType.MULTI_POLYGON_LAYER);
    config.setIdField(fid);
    config.setShapeField(shapeField);
    config.setAttributes(getProvinceFields());

    LayerReader<MultiPolygonLayer> provinceLayerReader = LayerFactory.getReader(ss, config);
    MultiPolygonLayer provinceLayer = provinceLayerReader.read();

    provinceLayer.makeSureCached();
    System.out.println("Layer Count: " + provinceLayer.count());

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridConfig(4, 2));
    KeyIndexedLayer<MultiPolygonLayer> indexedProvinceLayer = si.index(provinceLayer);
    indexedProvinceLayer.makeSureCached();

    System.out.println("Indexed Layer Count: " + indexedProvinceLayer.getLayer().count());

    KeyIndexedLayer<MultiPolygonLayer> r0 = indexedProvinceLayer.query(QueryGeometry);
    List<Tuple2<String, MultiPolygon>> l0 = r0.getLayer().collect();
    printInfo(String.format("Count Right: %d", l0.size()));

    InnerSpatialIndex isi = SpatialIndexFactory.getInnerSpatialIndex(IndexType.RTREE);
    PartitionIndexedLayer<MultiPolygonLayer, KeyIndexedLayer<MultiPolygonLayer>> paritionIndexedProvinceLayer = isi.index(indexedProvinceLayer);
    paritionIndexedProvinceLayer.makeSureCached();

    // 查询各个格网内部的数据
    printInfo(" ===== INDEXED LAYER START ===== ");
    KeyIndexedLayer<MultiPolygonLayer> r = paritionIndexedProvinceLayer.query(QueryGeometry);
    long count = r.getLayer().count();
    if (count == 17) printInfo(String.format("Count Right: %d", 17));
    else printInfo(String.format("Count Wrong: %d", count));
//    l.forEach((k, v)-> printInfo(String.format("GID: %s; Feature: %s", k, v.toString())));
    printInfo(" ===== INDEXED LAYER FINISH ===== ");

    ss.stop();
    ss.close();
  }

  private static Field[] getProvinceFields() {
    Field xzqdm = new Field("XZQDM","行政区代码", 2, FieldType.NORMAL_FIELD);
    Field xzqmc = new Field("XZQMC", "行政区名称", 3, FieldType.NORMAL_FIELD);
    return new Field[]{xzqdm, xzqmc};
  }

  public static void printInfo(String s) {
    System.out.println(s);
  }

}
