package edu.zju.gis.hls.ml.query;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import edu.zju.gis.hls.trajectory.analysis.model.MovingPoint;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.util.TWKTReader;
import org.apache.commons.collections.IteratorUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.tinspin.index.qtplain.QEntryDist;
import org.tinspin.index.qtplain.QuadTreeRKD0;
import sizeof.agent.SizeOfAgent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2021/1/11
 * 基于传统树形的线索引(和面索引是一样的)
 **/
public class PolylineTreeQuery {

  private static TWKTReader reader = new TWKTReader();
  private static Integer INPUT_DIMENSION = 3;
  private static String queryWkt =
    "POLYGON ((116.0 38.0, 117.0 38.0, 117.0 40.0, 116.0 40.0, 116.0 38.0))";
  private static Long[] TIME_WINDOW = { 1202249308000L, 1202433359000L };
  private static int KNN_NUM = 2;
  private static Point KNN_POINT = Point.create(116.5, 39.0, 1202249309000L);

  public static void main(String[] args) throws IOException, ParseException {

    String fileInput = "D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\trajectory\\data";

    List<MovingPoint> lines = Files.readAllLines(Paths.get(fileInput)).stream().map(new Function<String, MovingPoint>() {
      @Override
      public MovingPoint apply(String s) {
        String[] fields = s.split("\t");
        String id = fields[0];
        String wkt = fields[1];
        TemporalLineString tl = reader.readTemporalLineString(wkt.trim());
        return new MovingPoint(id, tl, null, null);
      }
    }).collect(Collectors.toList());

    MovingPoint[] points = new MovingPoint[lines.size()];
    points = lines.toArray(points);

    lines.clear();

    strTree(points);
    strStarTree(points);
    quadTree(points);
  }

  public static void strTree(MovingPoint[] records) throws IOException, ParseException {
    printInfo(" =========== strTree ========== ");
    List<Entry<Integer, Rectangle>> rl = new ArrayList<>();
    for (int i=0; i<records.length; i++) {
      MovingPoint record = records[i];
      Envelope e = record.getGeometry().getEnvelopeInternal();
      long[] timeRange = record.getTimeRange();
      Rectangle p = null;
      double[] rmins = new double[INPUT_DIMENSION];
      rmins[0] = e.getMinX();
      rmins[1] = e.getMinY();
      double[] rmaxs = new double[INPUT_DIMENSION];
      rmaxs[0] = e.getMaxX();
      rmaxs[1] = e.getMaxY();
      if (INPUT_DIMENSION == 3) {
        rmins[2] = timeRange[0];
        rmaxs[2] = timeRange[1];
      }
      p = Rectangle.create(rmins, rmaxs);
      rl.add(Entry.entry(i, p));
    }

    long startTime = System.currentTimeMillis();
    RTree<Integer, Rectangle> rTree = RTree.dimensions(INPUT_DIMENSION).create(rl);
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
    List<Entry<Integer, Rectangle>> result = IteratorUtils.toList(rTree.search(r).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<Entry<Integer, Rectangle>> knnResult = IteratorUtils.toList(rTree.nearest(KNN_POINT, Double.MAX_VALUE, KNN_NUM).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void strStarTree(MovingPoint[] records) throws IOException, ParseException {
    printInfo(" =========== strStarTree ========== ");
    List<Entry<Integer, Rectangle>> rl = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    for (int i=0; i<records.length; i++) {
      MovingPoint record = records[i];
      Envelope e = record.getGeometry().getEnvelopeInternal();
      long[] timeRange = record.getTimeRange();
      Rectangle p = null;
      double[] rmins = new double[INPUT_DIMENSION];
      rmins[0] = e.getMinX();
      rmins[1] = e.getMinY();
      double[] rmaxs = new double[INPUT_DIMENSION];
      rmaxs[0] = e.getMaxX();
      rmaxs[1] = e.getMaxY();
      if (INPUT_DIMENSION == 3) {
        rmins[2] = timeRange[0];
        rmaxs[2] = timeRange[1];
      }
      p = Rectangle.create(rmins, rmaxs);
      rl.add(Entry.entry(i, p));
    }
    RTree<Integer, Rectangle> rStarTree = RTree.star().dimensions(INPUT_DIMENSION).create(rl);
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
    List<Entry<Integer, Rectangle>> result = IteratorUtils.toList(rStarTree.search(r).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-*Tree Search Time: " + (endTime-startTime));
    printInfo(String.format("STR*-Tree Query Result: %s", result.size()));

    startTime = System.currentTimeMillis();
    List<Entry<Integer, Rectangle>> knnResult = IteratorUtils.toList(rStarTree.nearest(KNN_POINT, Double.MAX_VALUE, KNN_NUM).iterator());
    endTime = System.currentTimeMillis();
    printInfo("STR-Tree KNN Search Time: " + (endTime-startTime));
    printInfo(String.format("STRTree Query Result: %s", knnResult.size()));
  }

  public static void quadTree(MovingPoint[] records) throws IOException, ParseException {
    printInfo(" =========== quadTree ========== ");
    QuadTreeRKD0 qdTree = QuadTreeRKD0.create(INPUT_DIMENSION);
    long startTime = System.currentTimeMillis();
    for (int i=0; i<records.length; i++) {
      MovingPoint record = records[i];
      Envelope e = record.getGeometry().getEnvelopeInternal();
      long[] timeRange = record.getTimeRange();
      Rectangle p = null;
      double[] rmins = new double[INPUT_DIMENSION];
      rmins[0] = e.getMinX();
      rmins[1] = e.getMinY();
      double[] rmaxs = new double[INPUT_DIMENSION];
      rmaxs[0] = e.getMaxX();
      rmaxs[1] = e.getMaxY();
      if (INPUT_DIMENSION == 3) {
        rmins[2] = timeRange[0];
        rmaxs[2] = timeRange[1];
      }
      qdTree.insert(rmins, rmaxs, i);
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
    QuadTreeRKD0.QRIterator iterator = qdTree.queryIntersect(lower, upper);
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

  public static void printInfo(String s) {
    System.out.println(s);
  }

  public static long getSize(Object o) {
    return SizeOfAgent.fullSizeOf(o);
  }


}
