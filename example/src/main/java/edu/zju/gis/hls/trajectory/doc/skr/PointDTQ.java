package edu.zju.gis.hls.trajectory.doc.skr;

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
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.doc.skr.entity.CheckPoint;
import edu.zju.gis.hls.trajectory.doc.skr.entity.Region;
import edu.zju.gis.hls.trajectory.doc.sparktest.IndexStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import scala.Tuple2;
import scala.Tuple3;
import sizeof.agent.SizeOfAgent;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-18
 */
@Slf4j
public class PointDTQ {
    private static final String JDBC_URL = "jdbc:postgresql://10.79.231.84:5431/nyc-taxi-data";
    private static final IndexType GRID_INDEX_TYPE = IndexType.UNIFORM_GRID;
    private static final IndexConfig GRID_INDEX_CONFIG = new UniformGridConfig(8, 2);
    private static Integer INPUT_DIMENSION, KNN_NUM;
    private static BufferedWriter WRITER;
    private static TestData testData;

    static {
        try {
            testData = new TestData();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        // 初始化参数
        if (args.length != 3) {
            log.error("Args Error!");
        }
        INPUT_DIMENSION = Integer.parseInt(args[0].trim());
        KNN_NUM = Integer.parseInt(args[1].trim());
        WRITER = new BufferedWriter(new FileWriter(args[2].trim()));

        SparkConf sc = new SparkConf();
//        sc.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sc.set("spark.executor.extraJavaOptions", "-javaagent:D:/Resource/.m2/com/github/fracpete/sizeofag/1.0.4/sizeofag-1.0.4.jar");
//        sc.set("spark.kryo.register", "edu.zju.gis.hls.trajectory.doc.util.MyKryoRegister");
        SparkSession ss = SparkSession.builder()
                .master("local[4]")
                .appName("PointDTQ")
                .config(sc)
                .getOrCreate();

        TrajectoryPointLayer layer = readData(ss);

        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(GRID_INDEX_TYPE, GRID_INDEX_CONFIG);
        KeyIndexedLayer<TrajectoryPointLayer> indexedLayer = si.index(layer);
        TrajectoryPointLayer trajectoryPointLayer = indexedLayer.getLayer();
        SpatialPartitioner partitioner = indexedLayer.getPartitioner();

        trajectoryPointLayer.cache();
        WRITER.write("Trajectory Size: " + trajectoryPointLayer.count() + "\n\n");

        strTree(trajectoryPointLayer, partitioner);

        WRITER.flush();
        WRITER.close();
        ss.stop();
        ss.close();
    }

    private static TrajectoryPointLayer readData(SparkSession ss) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Field indexField = new IntegerField("index");
        indexField.setIndex(0);
        Field timeField = new LongField("pickup_datetime");
        timeField.setIndex(1);
        timeField.setFieldType(FieldType.TIME_FIELD);
        Field shapeField = new Field("pickup_geom");
        shapeField.setIndex(2);
        shapeField.setFieldType(FieldType.SHAPE_FIELD);

        LayerReaderConfig config = new PgLayerReaderConfig();
        config.setIdField(indexField);
        config.setSourcePath(JDBC_URL);
        config.setLayerId("TAXI_OD");
        config.setLayerName("TAXI_OD_LAYER");
        config.setCrs(Term.DEFAULT_CRS);
        config.setLayerType(LayerType.TRAJECTORY_POINT_LAYER);
        config.setShapeField(shapeField);
        config.setTimeField(timeField);
        config.setAttributes(new Field[]{indexField});

        LayerReader<TrajectoryPointLayer> reader = LayerFactory.getReader(ss, config);
        return reader.read();
    }

    private static void strTree(TrajectoryPointLayer trajectoryPointLayer, SpatialPartitioner partitioner) throws IOException {
        WRITER.write(" =========== STR-Tree ========== \n");
        long startTime = System.currentTimeMillis();
        JavaPairRDD<String, Tuple2<RTree, IndexStats>> innerTreeIndex = trajectoryPointLayer.mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, String, Tuple2<RTree, IndexStats>>) in -> {
            List<Tuple2<String, Tuple2<RTree, IndexStats>>> result = new ArrayList<>();
            List<Entry<Integer, Point>> rl = new ArrayList<>();
            List<Tuple2<String, TimedPoint>> records = IteratorUtils.toList(in);
            if (records.size() == 0) return result.iterator();
            String gridId = null;
            for (int i=0; i<records.size(); i++) {
                Tuple2<String, TimedPoint> record = records.get(i);
                if (gridId == null) gridId = record._1();
                double x = record._2().getGeometry().getX();
                double y = record._2().getGeometry().getY();
                Point p = INPUT_DIMENSION == 3 ? Point.create(x, y, (double) record._2().getTimestamp()) : Point.create(x, y);
                rl.add(Entry.entry(i, p));
            }

            long startTime1 = System.currentTimeMillis();
            RTree<Integer, Point> rTree = RTree.dimensions(INPUT_DIMENSION).create(rl);
            long endTime1 = System.currentTimeMillis();

            log.info("STR-Tree Bulk-Load Index Building Time: " + (endTime1- startTime1));
            log.info("STR-Tree Index Size: " + getSize(rTree));
            IndexStats stats = new IndexStats((long) records.size(), (endTime1- startTime1), getSize(rTree));
            result.add(new Tuple2<>(gridId, new Tuple2<>(rTree, stats)));
            return result.iterator();
        }).cache();

        WRITER.write("Indexed Layer Count: " + innerTreeIndex.count() + "\n");
        long endTime = System.currentTimeMillis();
        WRITER.write("Indexed Generate Time: " + (endTime - startTime) + "\n");

        trajectoryPointLayer.release();

        StringBuilder builder = new StringBuilder();

        // 区域查询测试
        WRITER.write(" ----------- Range Search Test ---------- \n");
        List<Region> regions = testData.getRegions();
        WRITER.write("level,index,time,resultSize\n");
        for (Region region : regions) {
            Rectangle r = Rectangle.create(region.getMin(INPUT_DIMENSION), region.getMax(INPUT_DIMENSION));
            List<String> gridKeys = partitioner.getKey(region.getGeometry());

            startTime = System.currentTimeMillis();
            JavaPairRDD<String, Tuple2<Integer, Point>> rangeQueryRDD = innerTreeIndex
                    .filter(x -> gridKeys.contains(x._1()))
                    .flatMapToPair((PairFlatMapFunction<Tuple2<String, Tuple2<RTree, IndexStats>>, String, Tuple2<Integer, Point>>) in -> {
                        List<Entry<Integer, Point>> searchResult = IteratorUtils.toList(in._2()._1().search(r).iterator());
                        List<Tuple2<Integer, Point>> result = searchResult.stream()
                                .map(x -> new Tuple2<>(x.value(), x.geometry()))
                                .collect(Collectors.toList());
                        return result.stream()
                                .map(x -> new Tuple2<>(in._1(), x))
                                .collect(Collectors.toList())
                                .iterator();
                    });
            List<Tuple2<String, Tuple2<Integer, Point>>> rangeQueryResult = rangeQueryRDD.collect();
            endTime = System.currentTimeMillis();

            builder.append(region.getLevel()).append(",").append(region.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(rangeQueryResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }

        // KNN查询测试
        WRITER.write(" ----------- KNN Search Test ---------- \n");
        List<CheckPoint> points = testData.getPoints();
        WRITER.write("index,time,resultSize\n");
        for (CheckPoint point : points) {
            startTime = System.currentTimeMillis();
            JavaPairRDD<String, Tuple3<Integer, Point, Double>> knnQueryRDD = innerTreeIndex
                    .flatMapToPair((PairFlatMapFunction<Tuple2<String, Tuple2<RTree, IndexStats>>, String, Tuple3<Integer, Point, Double>>) in -> {
                        List<Entry<Integer, Point>> searchResult = IteratorUtils.toList(in._2()._1().nearest(point.getKNNPoint(), Double.MAX_VALUE, KNN_NUM).iterator());
                        List<Tuple3<Integer, Point, Double>> result = searchResult.stream()
                                .map(x->new Tuple3<>(x.value(), x.geometry(), x.geometry().distance(point.getKNNPoint().mbr())))
                                .collect(Collectors.toList());
                        return result.stream()
                                .map(x -> new Tuple2<>(in._1(), x))
                                .collect(Collectors.toList()).iterator();
                    });
            List<Tuple2<String, Tuple3<Integer, Point, Double>>> knnResult = new ArrayList<>(knnQueryRDD.collect());
            knnResult.sort(Comparator.comparing(o -> o._2()._3()));
            knnResult = knnResult.subList(0, 2);
            endTime = System.currentTimeMillis();

            builder.append(point.getIndex()).append(",");
            builder.append(endTime-startTime).append(",").append(knnResult.size()).append("\n");
            WRITER.write(builder.toString());
            builder.delete(0, builder.length());
        }
    }

    private static long getSize(Object o) {
        return SizeOfAgent.fullSizeOf(o);
    }
}
