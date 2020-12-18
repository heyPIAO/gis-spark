package edu.zju.gis.hls.trajectory.doc.preprocess;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.uniformGrid.UniformGridConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;



/**
 * @author Hu
 * @date 2020/12/8
 * 二维点数据基于 hilbert curve 和 x 精度的训练数据生成
 **/
public class H2PointTrainData {

  private static String TDRIVE_DIR = "/home/DaLunWen/data/trajectory/test_10_wkt";
  private static String FILE_TYPE = "";

  public static void main(String[] args) throws Exception {
    generate();
  }

  // 用10条T-drive出租车数据生成训练数据
  private static void generate() throws Exception {

    SparkSession ss = SparkSession.builder().master("local[1]").appName("GenerateTrainData").getOrCreate();
    TrajectoryPointLayer layer = readData(ss);
    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridConfig(4, 2));
    KeyIndexedLayer<TrajectoryPointLayer> indexedLayer = si.index(layer);
    TrajectoryPointLayer trajectoryPointLayer = indexedLayer.getLayer();
    JavaPairRDD<String, Iterable<TimedPoint>> layer2 = trajectoryPointLayer.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,TimedPoint>>, Object>() {
    })


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
    TrajectoryPointLayer layer = reader.read();

    return layer;
  }

}
