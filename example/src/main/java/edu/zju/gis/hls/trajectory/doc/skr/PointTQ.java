package edu.zju.gis.hls.trajectory.doc.skr;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord;
import edu.zju.gis.hls.trajectory.doc.skr.entity.CheckPoint;
import edu.zju.gis.hls.trajectory.doc.skr.entity.Region;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import org.tinspin.index.kdtree.KDIterator;
import org.tinspin.index.kdtree.KDTree;
import org.tinspin.index.qtplain.QEntryDist;
import org.tinspin.index.qtplain.QuadTreeKD0;
import sizeof.agent.SizeOfAgent;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-15
 */
@Slf4j
public class PointTQ {

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
                .option("url", "jdbc:postgresql://10.79.231.84:5431/nyc-taxi-data")
                .option("dbtable", "trips")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("continueBatchOnError", true)
                .option("pushDownPredicate", true) // 默认请求下推
                .load();
        JavaRDD<TrainingRecord> recordsRDD = temporalData.javaRDD().map(TrainingRecord::new);
        List<TrainingRecord> recordsList = recordsRDD.collect();

        ss.stop();
        ss.close();

        TrainingRecord[] records = new TrainingRecord[recordsList.size()];
        records = recordsList.toArray(records);

        WRITER.write("Total Data Size: " + records.length + "\n");

        recordsList = null;

        strTree(records);
        strStarTree(records);
        quadTree(records);
        kdTree(records);

        WRITER.flush();
        WRITER.close();
    }

    public static void strTree(TrainingRecord[] records) throws IOException {
        WRITER.write(" =========== STR-Tree ========== \n");
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

    public static void strStarTree(TrainingRecord[] records) throws IOException {
        WRITER.write(" =========== STR*-Tree ========== \n");
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

    public static void quadTree(TrainingRecord[] records) throws IOException {
        WRITER.write(" =========== Quad-Tree ========== \n");
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
        WRITER.write("QuadTree Index Building Time: " + (endTime-startTime) + "\n");
        WRITER.write("QuadTree Index Size: " + getSize(qdTree) + "\n");

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            startTime = System.currentTimeMillis();
            QuadTreeKD0.QIterator iterator = qdTree.query(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
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

    public static void kdTree(TrainingRecord[] records) throws IOException {
        WRITER.write(" =========== kdTree ========== \n");
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
        WRITER.write("KdTree Index Building Time: " + (endTime-startTime) + "\n");
        WRITER.write("KdTree Index Size: " + getSize(kdTree) + "\n");

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            startTime = System.currentTimeMillis();
            KDIterator iterator = kdTree.query(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
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
            List<QEntryDist> knnResult = kdTree.knnQuery(point.getValue(), KNN_NUM);
            endTime = System.currentTimeMillis();
            builder.append(point.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(knnResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }
    }

    public static void printInfo(String s) {
        System.out.println(s);
    }

    public static long getSize(Object o) {
        return SizeOfAgent.fullSizeOf(o);
    }

}
