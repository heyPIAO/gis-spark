package edu.zju.gis.hls.trajectory.doc.skr;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import edu.zju.gis.hls.trajectory.doc.model.OsmRecord;
import edu.zju.gis.hls.trajectory.doc.skr.entity.CheckPoint;
import edu.zju.gis.hls.trajectory.doc.skr.entity.Region;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import org.tinspin.index.qtplain.QEntryDist;
import org.tinspin.index.qtplain.QuadTreeRKD0;
import sizeof.agent.SizeOfAgent;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-18
 */
@Slf4j
public class PolygonTQ {
    private static Integer INPUT_DIMENSION;
    private static Integer KNN_NUM;
    private static BufferedWriter WRITER;
    private static TestData testData;

    static {
        try {
            testData = new TestData();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ParseException, java.text.ParseException {
        // 初始化参数
        if (args.length != 3) {
            log.error("Args Error!");
        }
        INPUT_DIMENSION = Integer.parseInt(args[0].trim());
        KNN_NUM = Integer.parseInt(args[1].trim());
        WRITER = new BufferedWriter(new FileWriter(args[2].trim()));

        // 读取数据，构建序列
        SparkSession ss = SparkSession.builder().master("local[1]").appName("PointQuery").getOrCreate();
        Dataset<Row> temporalData = ss.read().format("jdbc")
                .option("url", "jdbc:postgresql://10.79.231.84:5431/gis")
                .option("dbtable", "planet_osm_polygon")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("continueBatchOnError", true)
                .option("pushDownPredicate", true) // 默认请求下推
                .load();
        JavaRDD<OsmRecord> recordsRDD = temporalData.javaRDD().map(OsmRecord::new);
        List<OsmRecord> recordsList = recordsRDD.collect();

        ss.stop();
        ss.close();

        OsmRecord[] records = new OsmRecord[recordsList.size()];
        records = recordsList.toArray(records);

        WRITER.write("Total Data Size: " + records.length + "\n");

        recordsList.clear();

        Rectangle[] rectangles = dataPreProcessing(records);
        strTree(rectangles);
        strStarTree(rectangles);
        quadTree(rectangles);

        WRITER.flush();
        WRITER.close();
    }

    private static Rectangle[] dataPreProcessing(OsmRecord[] records) {
        Rectangle[] rectangles = new Rectangle[records.length];
        for (int i = 0; i < records.length; i++) {
            OsmRecord record = records[i];
            double[] rmins = new double[INPUT_DIMENSION];
            rmins[0] = record.getEnvelope().getMinX();
            rmins[1] = record.getEnvelope().getMinY();
            double[] rmaxs = new double[INPUT_DIMENSION];
            rmaxs[0] = record.getEnvelope().getMaxX();
            rmaxs[1] = record.getEnvelope().getMaxY();
            if (INPUT_DIMENSION == 3) {
                rmins[2] = System.currentTimeMillis();
                rmaxs[2] = System.currentTimeMillis();
            }
            rectangles[i] = Rectangle.create(rmins, rmaxs);
        }
        return rectangles;
    }

    public static void strTree(Rectangle[] rectangles) throws IOException {
        WRITER.write(" =========== STR-Tree ========== \n");
        List<Entry<Integer, Rectangle>> rl = new ArrayList<>();
        for (int i = 0; i < rectangles.length; i++) {
            rl.add(Entry.entry(i, rectangles[i]));
        }

        long startTime = System.currentTimeMillis();
        RTree<Integer, Rectangle> rTree = RTree.dimensions(INPUT_DIMENSION).create(rl);
        long endTime = System.currentTimeMillis();
        WRITER.write("STR-Tree Bulk-Load Index Building Time: " + (endTime-startTime) + "\n");
        WRITER.write("STR-Tree Index Size: " + getSize(rTree) + "\n");

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            Rectangle r = Rectangle.create(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
            startTime = System.currentTimeMillis();
            List<Entry<Integer, Point>> result = IteratorUtils.toList(rTree.search(r).iterator());
            endTime = System.currentTimeMillis();
            builder.append(region.getLevel()).append(",").append(region.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(result.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }

        // KNN查询测试
        WRITER.write(" ----------- KNN Search Test ---------- \n");
        List<CheckPoint> points = testData.getPoints();
        WRITER.write("index,time,resultSize\n");
        for (CheckPoint point : points) {
            startTime = System.currentTimeMillis();
            List<Entry<Integer, Point>> knnResult = IteratorUtils.toList(rTree.nearest(point.getKNNPoint(), Double.MAX_VALUE, KNN_NUM).iterator());
            endTime = System.currentTimeMillis();
            builder.append(point.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(knnResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }
    }

    public static void strStarTree(Rectangle[] rectangles) throws IOException {
        WRITER.write(" =========== STR*-Tree ========== \n");
        List<Entry<Integer, Rectangle>> rl = new ArrayList<>();
        for (int i = 0; i < rectangles.length; i++) {
            rl.add(Entry.entry(i, rectangles[i]));
        }

        long startTime = System.currentTimeMillis();
        RTree<Integer, Rectangle> rStarTree = RTree.star().dimensions(INPUT_DIMENSION).create(rl);
        long endTime = System.currentTimeMillis();
        WRITER.write("STR*-Tree Bulk-Load Index Building Time: " + (endTime-startTime) + "\n");
        WRITER.write("STR*-Tree Index Size: " + getSize(rStarTree) + "\n");

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            Rectangle r = Rectangle.create(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
            startTime = System.currentTimeMillis();
            List<Entry<Integer, Point>> result = IteratorUtils.toList(rStarTree.search(r).iterator());
            endTime = System.currentTimeMillis();
            builder.append(region.getLevel()).append(",").append(region.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(result.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }

        // KNN查询测试
        WRITER.write(" ----------- KNN Search Test ---------- \n");
        List<CheckPoint> points = testData.getPoints();
        WRITER.write("index,time,resultSize\n");
        for (CheckPoint point : points) {
            startTime = System.currentTimeMillis();
            List<Entry<Integer, Point>> knnResult = IteratorUtils.toList(rStarTree.nearest(point.getKNNPoint(), Double.MAX_VALUE, KNN_NUM).iterator());
            endTime = System.currentTimeMillis();
            builder.append(point.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(knnResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }
    }

    public static void quadTree(Rectangle[] rectangles) throws IOException {
        WRITER.write(" =========== Quad-Tree ========== \n");
        QuadTreeRKD0 qdTree = QuadTreeRKD0.create(INPUT_DIMENSION);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < rectangles.length; i++) {
            qdTree.insert(rectangles[i].mins(), rectangles[i].maxes(), i);
        }
        long endTime = System.currentTimeMillis();
        WRITER.write("QuadTree Index Building Time: " + (endTime-startTime) + "\n");
        WRITER.write("QuadTree Index Size: " + getSize(qdTree) + "\n");

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            startTime = System.currentTimeMillis();
            QuadTreeRKD0.QRIterator iterator = qdTree.queryIntersect(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
            endTime = System.currentTimeMillis();
            List result = IteratorUtils.toList(iterator);
            builder.append(region.getLevel()).append(",").append(region.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(result.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }

        // KNN查询测试
        WRITER.write(" ----------- KNN Search Test ---------- \n");
        List<CheckPoint> points = testData.getPoints();
        WRITER.write("index,time,resultSize\n");
        for (CheckPoint point : points) {
            startTime = System.currentTimeMillis();
            List<QEntryDist> knnResult = qdTree.knnQuery(point.getValue(), KNN_NUM);
            endTime = System.currentTimeMillis();
            builder.append(point.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(knnResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }
    }

    public static long getSize(Object o) {
        return SizeOfAgent.fullSizeOf(o);
    }
}
