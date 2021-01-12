package edu.zju.gis.hls.ml.query;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.tinspin.index.kdtree.KDIterator;
import org.tinspin.index.kdtree.KDTree;
import org.tinspin.index.qtplain.QEntryDist;
import org.tinspin.index.qtplain.QuadTreeKD0;
import sizeof.agent.SizeOfAgent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2021/1/11
 * 基于传统树形的点索引
 **/
public class PointTreeQuery {

  private static Integer INPUT_DIMENSION = 3;
  private static String queryWkt =
    "POLYGON ((116.0 38.0, 117.0 38.0, 117.0 40.0, 116.0 40.0, 116.0 38.0))";
  private static Long[] TIME_WINDOW = { 1202249308000L, 1202433359000L };
  private static int KNN_NUM = 2;
  private static Point KNN_POINT = Point.create(116.5, 39.0, 1202249309000L);

  public static void main(String[] args) throws IOException, ParseException {

    String filePath = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\spatialPoint\\single_dim_x\\*";

    // 读取数据，构建序列
    SparkSession ss = SparkSession.builder().master("local[1]").appName("PointQuery").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
    JavaRDD<TrainingRecord> recordsRDD = jsc.textFile(filePath).map(TrainingRecord::new);
    List<TrainingRecord> recordsList = recordsRDD.collect();

    ss.stop();
    ss.close();

    TrainingRecord[] records = new TrainingRecord[recordsList.size()];
    records = recordsList.toArray(records);

    printInfo("Total Data Size: " + records.length);

    recordsList = null;

    strTree(records);
    strStarTree(records);
    quadTree(records);
    kdTree(records);
  }

  public static void strTree(TrainingRecord[] records) throws IOException, ParseException {
    printInfo(" =========== strTree ========== ");
    List<Entry<Integer, Point>> rl = new ArrayList<>();
    for (int i=0; i<records.length; i++) {
      TrainingRecord record = records[i];
      Point p = null;
      if (INPUT_DIMENSION == 3) {
        p = Point.create(record.getX(), record.getY(), record.getT());
      } else {
        p = Point.create(record.getX(), record.getY());
      }
      rl.add(Entry.entry(i, p));
    }
    long startTime = System.currentTimeMillis();
    RTree<Integer, Point> rTree = RTree.dimensions(INPUT_DIMENSION).create(rl);
    long endTime = System.currentTimeMillis();
    printInfo("STR-Tree Bulk-Load Index Building Time: " + (endTime-startTime));
    printInfo("STR-Tree Index Size: " + getSize(rTree));
    // 查询测试
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
    List<Entry<Integer, Point>> result = IteratorUtils.toList(rTree.search(r).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<Entry<Integer, Point>> knnResult = IteratorUtils.toList(rTree.nearest(KNN_POINT, Double.MAX_VALUE, KNN_NUM).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void strStarTree(TrainingRecord[] records) throws IOException, ParseException {
    printInfo(" =========== strStarTree ========== ");
    List<Entry<Integer, Point>> rl = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    for (int i=0; i<records.length; i++) {
      TrainingRecord record = records[i];
      Point p = null;
      if (INPUT_DIMENSION == 3) {
        p = Point.create(record.getX(), record.getY(), record.getT());
      } else {
        p = Point.create(record.getX(), record.getY());
      }
      rl.add(Entry.entry(i, p));
    }
    RTree<Integer, Point> rStarTree = RTree.star().dimensions(INPUT_DIMENSION).create(rl);
    long endTime = System.currentTimeMillis();
    printInfo("STR*-Tree Bulk-Load Index Building Time: " + (endTime-startTime));
    printInfo("STR*-Tree Index Size: " + getSize(rStarTree));

    // 查询测试
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
    List<Entry<Integer, Point>> result = IteratorUtils.toList(rStarTree.search(r).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-*Tree Search Time: " + (endTime-startTime));
    printInfo(String.format("STR*-Tree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<Entry<Integer, Point>> knnResult = IteratorUtils.toList(rStarTree.nearest(KNN_POINT, Double.MAX_VALUE, KNN_NUM).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void quadTree(TrainingRecord[] records) throws IOException, ParseException {
    printInfo(" =========== quadTree ========== ");
    QuadTreeKD0 qdTree = QuadTreeKD0.create(INPUT_DIMENSION);
    long startTime = System.currentTimeMillis();
    for (int i=0; i<records.length; i++) {
      TrainingRecord record = records[i];
      double[] t = new double[INPUT_DIMENSION];
      t[0] = record.getX();
      t[1] = record.getY();
      if (INPUT_DIMENSION == 3) t[2] = record.getT();
      qdTree.insert(t, i);
    }
    long endTime = System.currentTimeMillis();
    printInfo("QuadTree Index Building Time: " + (endTime-startTime));
    printInfo("QuadTree Index Size: " + getSize(qdTree));
    // 查询测试
    WKTReader reader = new WKTReader();
    Geometry queryGeometry = reader.read(queryWkt);
    Envelope e = queryGeometry.getEnvelopeInternal();
    double[] lower = new double[INPUT_DIMENSION];
    lower[0] = e.getMinX();
    lower[1] = e.getMinY();
    if (INPUT_DIMENSION == 3) lower[2] = TIME_WINDOW[0];
    double[] upper = new double[INPUT_DIMENSION];
    upper[0] = e.getMaxX();
    upper[1] = e.getMaxY();
    if (INPUT_DIMENSION == 3) upper[2] = TIME_WINDOW[1];
    startTime = System.currentTimeMillis();
    QuadTreeKD0.QIterator iterator = qdTree.query(lower, upper);
    endTime = System.currentTimeMillis();
    printInfo("QuadTree Search Time: " + (endTime-startTime));
    List result = IteratorUtils.toList(iterator);
    printInfo(String.format("QuadTree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<QEntryDist> knnResult = qdTree.knnQuery(KNN_POINT.values(), KNN_NUM);
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void kdTree(TrainingRecord[] records) throws IOException, ParseException {
    printInfo(" =========== kdTree ========== ");
    KDTree kdTree = KDTree.create(INPUT_DIMENSION);
    long startTime = System.currentTimeMillis();
    for (int i=0; i<records.length; i++) {
      TrainingRecord record = records[i];
      double[] t = new double[INPUT_DIMENSION];
      t[0] = record.getX();
      t[1] = record.getY();
      if (INPUT_DIMENSION == 3) t[2] = record.getT();
      kdTree.insert(t, i);
    }
    long endTime = System.currentTimeMillis();
    printInfo("KdTree Index Building Time: " + (endTime-startTime));
    printInfo("KdTree Index Size: " + getSize(kdTree));
    // 查询测试
    WKTReader reader = new WKTReader();
    Geometry queryGeometry = reader.read(queryWkt);
    Envelope e = queryGeometry.getEnvelopeInternal();
    double[] lower = new double[INPUT_DIMENSION];
    lower[0] = e.getMinX();
    lower[1] = e.getMinY();
    if (INPUT_DIMENSION == 3) lower[2] = TIME_WINDOW[0];
    double[] upper = new double[INPUT_DIMENSION];
    upper[0] = e.getMaxX();
    upper[1] = e.getMaxY();
    if (INPUT_DIMENSION == 3) upper[2] = TIME_WINDOW[1];
    startTime = System.currentTimeMillis();
    KDIterator iterator = kdTree.query(lower, upper);
    endTime = System.currentTimeMillis();
    printInfo("KD-Tree Search Time: " + (endTime-startTime));
    List result = IteratorUtils.toList(iterator);
    printInfo(String.format("KdTree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<QEntryDist> knnResult = kdTree.knnQuery(KNN_POINT.values(), KNN_NUM);
    endTime = System.currentTimeMillis();
    printInfo("KDTree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void printInfo(String s) {
    System.out.println(s);
  }

  public static long getSize(Object o) {
    return SizeOfAgent.fullSizeOf(o);
  }

}
