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
public class InnerMLIndexTest {

  private static String TDRIVE_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\test_10_wkt";

  public static void main(String[] args) throws Exception {

    String queryWkt =
      "POLYGON ((116.0 38.0, 117.0 38.0, 117.0 40.0, 116.0 40.0, 116.0 38.0))";
    WKTReader reader = new WKTReader();
    Geometry QueryGeometry = reader.read(queryWkt);

    SparkSession ss = SparkSession.builder()
      .master("local[1]")
      .appName("edu.zju.gis.hls.trajectory.analysis.index.ml.InnerMLIndexTest")
      .getOrCreate();

    TrajectoryPointLayer trajectoryLayer = readData(ss);

    trajectoryLayer.makeSureCached();
    System.out.println("Layer Count: " + trajectoryLayer.count());

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridConfig(4, 2));
    KeyIndexedLayer<TrajectoryPointLayer> indexedTrajectoryLayer = si.index(trajectoryLayer);
    indexedTrajectoryLayer.makeSureCached();

    System.out.println("Indexed Layer Count: " + indexedTrajectoryLayer.getLayer().count());

    KeyIndexedLayer<TrajectoryPointLayer> r0 = indexedTrajectoryLayer.query(QueryGeometry);
    List<Tuple2<String, TimedPoint>> l0 = r0.getLayer().collect();
    printInfo(String.format("Count Right: %d", l0.size()));

    InnerSpatialIndex isi = SpatialIndexFactory.getInnerSpatialIndex(IndexType.NN);
    PartitionIndexedLayer<TrajectoryPointLayer, KeyIndexedLayer<TrajectoryPointLayer>> paritionIndexedTrajectoryLayer
      = isi.index(indexedTrajectoryLayer);
    paritionIndexedTrajectoryLayer.makeSureCached();

    // 查询各个格网内部的数据
    printInfo(" ===== INDEXED LAYER START ===== ");
    KeyIndexedLayer<TrajectoryPointLayer> r = paritionIndexedTrajectoryLayer.query(QueryGeometry);
    long count = r.getLayer().count();
    if (count == l0.size()) printInfo(String.format("Inner Index Count Right: %d", l0.size()));
    else printInfo(String.format("Count Wrong: %d", count));
    printInfo(" ===== INDEXED LAYER FINISH ===== ");

    ss.stop();
    ss.close();
  }

  public static void printInfo(String s) {
    System.out.println(s);
  }

  private static TrajectoryPointLayer readData(SparkSession ss) throws Exception {
    Field trajId = new IntegerField("trajId");
    trajId.setIndex(0);
    Field timeField = new LongField("time");
    timeField.setIndex(1);
    timeField.setFieldType(FieldType.TIME_FIELD);
    Field shapeField = new Field("shape");
    shapeField.setIndex(2);
    shapeField.setFieldType(FieldType.SHAPE_FIELD);

    LayerReaderConfig config = new FileLayerReaderConfig();
    config.setSourcePath(TDRIVE_DIR);
    config.setLayerId("TDRIVE_LAYER");
    config.setLayerName("TDRIVE_LAYER");
    config.setCrs(Term.DEFAULT_CRS);
    config.setLayerType(LayerType.TRAJECTORY_POINT_LAYER);
    config.setShapeField(shapeField);
    config.setTimeField(timeField);
    config.setAttributes(new Field[]{trajId});

    LayerReader<TrajectoryPointLayer> reader = LayerFactory.getReader(ss, config);
    return reader.read();
  }

}

