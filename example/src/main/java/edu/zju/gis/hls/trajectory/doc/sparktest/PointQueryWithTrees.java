package edu.zju.gis.hls.trajectory.doc.sparktest;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.index.uniformGrid.UniformGridConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;
import scala.Tuple3;
import sizeof.agent.SizeOfAgent;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2021/1/12
 * 分布式点数据检索，包括二维和三维点
 * 支持各类格网
 **/
public class PointQueryWithTrees {

  private static String TDRIVE_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\test_10_wkt";
  private static IndexType GRID_INDEX_TYPE = IndexType.UNIFORM_GRID;
  private static IndexConfig GRID_INDEX_CONFIG = new UniformGridConfig(8, 2);
  private static Integer INPUT_DIMENSION = 3;
  private static String queryWkt =
    "POLYGON ((116.0 38.0, 117.0 38.0, 117.0 40.0, 116.0 40.0, 116.0 38.0))";
  private static Long[] TIME_WINDOW = { 1202249308000L, 1202433359000L };
  private static int KNN_NUM = 2;
  private static Point KNN_POINT = Point.create(116.5, 39.0, 1202249309000L);

  public static void main(String[] args) throws Exception {

    SparkConf sc = new SparkConf();
//    sc.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
    sc.set("spark.executor.extraJavaOptions", "-javaagent:D:/Resource/.m2/com/github/fracpete/sizeofag/1.0.4/sizeofag-1.0.4.jar");
//    sc.set("spark.kryo.register", "edu.zju.gis.hls.trajectory.doc.util.MyKryoRegister");
    SparkSession ss = SparkSession.builder().master("local[4]").appName("PointQueryWithTrees")
      .config(sc)
      .getOrCreate();
    TrajectoryPointLayer layer = readData(ss);

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(GRID_INDEX_TYPE, GRID_INDEX_CONFIG);
    KeyIndexedLayer<TrajectoryPointLayer> indexedLayer = si.index(layer);
    TrajectoryPointLayer trajectoryPointLayer = indexedLayer.getLayer();
    SpatialPartitioner partitioner = indexedLayer.getPartitioner();

    trajectoryPointLayer.cache();

    printInfo("Trajectory Size: " + trajectoryPointLayer.count());

    long startTime = System.currentTimeMillis();
    JavaPairRDD<String, Tuple2<RTree, IndexStats>> innerTreeIndex = trajectoryPointLayer.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, String, Tuple2<RTree, IndexStats>>() {
      @Override
      public Iterator<Tuple2<String, Tuple2<RTree, IndexStats>>> call(Iterator<Tuple2<String, TimedPoint>> in) throws Exception {
        List<Tuple2<String, Tuple2<RTree, IndexStats>>> result = new ArrayList<>();
        List<Entry<Integer, Point>> rl = new ArrayList<>();
        List<Tuple2<String, TimedPoint>> records = IteratorUtils.toList(in);
        if (records.size() == 0) return result.iterator();
        String gridId = null;
        for (int i=0; i<records.size(); i++) {
          Tuple2<String, TimedPoint> record = records.get(i);
          if (gridId == null) gridId = record._1;
          double x = record._2.getGeometry().getX();
          double y = record._2.getGeometry().getY();
          Point p = null;
          if (INPUT_DIMENSION == 3) p = Point.create(x, y, Double.valueOf(record._2.getTimestamp()));
          else p = Point.create(x, y);
          rl.add(Entry.entry(i, p));
        }
        long startTime = System.currentTimeMillis();
        RTree<Integer, Point> rTree = RTree.dimensions(INPUT_DIMENSION).create(rl);
        long endTime = System.currentTimeMillis();
        printInfo("STR-Tree Bulk-Load Index Building Time: " + (endTime-startTime));
        printInfo("STR-Tree Index Size: " + getSize(rTree));

        IndexStats stats = new IndexStats(Long.valueOf(records.size()), (endTime-startTime), getSize(rTree));
        result.add(new Tuple2<>(gridId, new Tuple2<>(rTree, stats)));

        return result.iterator();
      }
    });

    innerTreeIndex.cache();
    printInfo("Indexed Layer Count: " + innerTreeIndex.count());
    long endTime = System.currentTimeMillis();
    printInfo("Indexed Generate Time: " + (endTime - startTime));

    trajectoryPointLayer.release();

    WKTReader reader = new WKTReader();
    Geometry queryGeometry = reader.read(queryWkt);
    Envelope e = queryGeometry.getEnvelopeInternal();
    double mins[] = new double[INPUT_DIMENSION];
    double maxs[] = new double[INPUT_DIMENSION];
    mins[0] = e.getMinX();
    mins[1] = e.getMinY();
    if (INPUT_DIMENSION == 3) mins[2] = TIME_WINDOW[0];
    maxs[0] = e.getMaxX();
    maxs[1] = e.getMaxY();
    if (INPUT_DIMENSION == 3) maxs[2] = TIME_WINDOW[1];
    Rectangle r = Rectangle.create(mins, maxs);

    startTime = System.currentTimeMillis();
    List<String> gridKeys = partitioner.getKey(queryGeometry);
    JavaPairRDD<String, Tuple2<Integer, Point>> rangeQueryRDD = innerTreeIndex.filter(x->gridKeys.contains(x._1)).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<RTree, IndexStats>>, String, Tuple2<Integer, Point>>() {
      @Override
      public Iterator<Tuple2<String, Tuple2<Integer, Point>>> call(Tuple2<String, Tuple2<RTree, IndexStats>> in) throws Exception {
        List<Entry<Integer, Point>> searchResult = IteratorUtils.toList(in._2._1.search(r).iterator());
        List<Tuple2<Integer, Point>> result = searchResult.stream().map(x->new Tuple2<>(x.value(), x.geometry())).collect(Collectors.toList());
        return result.stream().map(x -> new Tuple2<String, Tuple2<Integer, Point>>(in._1, x)).collect(Collectors.toList()).iterator();
      }
    });
    List<Tuple2<String, Tuple2<Integer, Point>>> rangeQueryResult = rangeQueryRDD.collect();
    endTime = System.currentTimeMillis();

    printInfo("STR-Tree Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", rangeQueryResult.size()));
    JavaPairRDD<String, Tuple3<Integer, Point, Double>> knnQueryRDD = innerTreeIndex.filter(x->gridKeys.contains(x._1)).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<RTree, IndexStats>>, String, Tuple3<Integer, Point, Double>>() {
      @Override
      public Iterator<Tuple2<String, Tuple3<Integer, Point, Double>>> call(Tuple2<String, Tuple2<RTree, IndexStats>> in) throws Exception {
        List<Entry<Integer, Point>> searchResult = IteratorUtils.toList(in._2._1.nearest(KNN_POINT, Double.MAX_VALUE, KNN_NUM).iterator());
        List<Tuple3<Integer, Point, Double>> result = searchResult.stream().map(x->new Tuple3<>(x.value(), x.geometry(), x.geometry().distance(KNN_POINT.mbr()))).collect(Collectors.toList());
        return result.stream().map(x -> new Tuple2<String, Tuple3<Integer, Point, Double>>(in._1, x)).collect(Collectors.toList()).iterator();
      }
    });
    List<Tuple2<String, Tuple3<Integer, Point, Double>>> knnResult = new ArrayList<>(knnQueryRDD.collect());
    knnResult.sort(new Comparator<Tuple2<String, Tuple3<Integer, Point, Double>>>() {
      @Override
      public int compare(Tuple2<String, Tuple3<Integer, Point, Double>> o1, Tuple2<String, Tuple3<Integer, Point, Double>> o2) {
        return o1._2._3().compareTo(o2._2._3());
      }
    });
    knnResult = knnResult.subList(0, 2);
    startTime = System.currentTimeMillis();

    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
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

  public static void printInfo(String s) {
    System.out.println(s);
  }

  public static long getSize(Object o) {
    return SizeOfAgent.fullSizeOf(o);
  }

}
