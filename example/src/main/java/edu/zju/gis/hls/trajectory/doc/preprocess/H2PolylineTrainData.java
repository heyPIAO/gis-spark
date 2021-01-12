package edu.zju.gis.hls.trajectory.doc.preprocess;

import edu.zju.gis.hls.trajectory.analysis.model.MovingPoint;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord3;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * @author Hu
 * @date 2020/12/8
 * 微软轨迹数据转为测试线数据
 * HINT：从已经归一化的点数据，根据trajId将其重新聚合并以wkt写出到文件
 **/
public class H2PolylineTrainData {

  public static void main(String[] args) {

    String filePath = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\temporalPoint\\multi_dim\\*";
    String outputPath = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\trajectory\\";

    // 读取数据，构建序列
    SparkSession ss = SparkSession.builder().master("local[1]").appName("GeneratePolylineData").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
    JavaPairRDD<String, TrainingRecord3> recordsRDD = jsc.textFile(filePath).map(TrainingRecord3::new).mapToPair(x->new Tuple2<>(x.getTrajId(), x));
    JavaRDD<MovingPoint> trajectoryRDD = recordsRDD.groupByKey().map(new Function<Tuple2<String, Iterable<TrainingRecord3>>, MovingPoint>() {
      @Override
      public MovingPoint call(Tuple2<String, Iterable<TrainingRecord3>> in) throws Exception {
        List<TrainingRecord3> records = IteratorUtils.toList(in._2.iterator());
        records.sort(Comparator.comparing(TrainingRecord3::getT));
        long[] instants = new long[records.size()];
        Coordinate[] coordinates = new Coordinate[records.size()];
        for (int i=0; i<records.size(); i++) {
          TrainingRecord3 record = records.get(i);
          coordinates[i] = new Coordinate(record.getX(), record.getY());
          instants[i] = record.getT();
        }
        CoordinateSequence cs = new CoordinateArraySequence(coordinates);
        TemporalLineString tline = new TemporalLineString(instants, cs);
        return new MovingPoint(in._1, tline, null, null);
      }
    });

    JavaRDD<String> resultRDD= trajectoryRDD.map(x->String.format("%s\t%s", x.getFid(), x.getGeometryWkt()));

    resultRDD.repartition(1).saveAsTextFile(outputPath);

    ss.stop();
    ss.close();
  }

}
