package edu.zju.gis.hls.trajectory.doc.preprocess;

import com.google.common.primitives.Bytes;
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
import edu.zju.gis.hls.trajectory.doc.util.IntegerUtils;
import edu.zju.gis.hls.trajectory.doc.util.LongUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2020/12/8
 * 二维点数据基于 hilbert curve 和 x 精度的训练数据生成
 **/
public class H2PointTrainData {

  private static String TDRIVE_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\test_10_wkt";
  private static String TDRIVE_OUT_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\out2";

  public static void main(String[] args) throws Exception {
    generate();
  }

  // 用10条T-drive出租车数据生成训练数据
  private static void generate() throws Exception {

    SparkSession ss = SparkSession.builder().master("local[4]").appName("GenerateTrainData").getOrCreate();
    TrajectoryPointLayer layer = readData(ss);

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridConfig(8, 2));
    KeyIndexedLayer<TrajectoryPointLayer> indexedLayer = si.index(layer);
    TrajectoryPointLayer trajectoryPointLayer = indexedLayer.getLayer();

    trajectoryPointLayer = (TrajectoryPointLayer) trajectoryPointLayer.mapPartitionsToLayer(new PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, String, TimedPoint>() {
      @Override
      public Iterator<Tuple2<String, TimedPoint>> call(Iterator<Tuple2<String, TimedPoint>> tuple2Iterator) throws Exception {
        // TODO 在格网内部，根据时间戳进行排序，并添加到index字段中
        if (!tuple2Iterator.hasNext()) return tuple2Iterator;
        List<Tuple2<String, TimedPoint>> result = IteratorUtils.toList(tuple2Iterator);
        List<Tuple2<String, TimedPoint>> output = new LinkedList<>();
        Collections.sort(result, new Comparator<Tuple2<String, TimedPoint>>() {
          @Override
          public int compare(Tuple2<String, TimedPoint> o1, Tuple2<String, TimedPoint> o2) {
            return o1._2.getTimestamp() > o2._2.getTimestamp() ? 1 : 0;
          }
        });
        for (int i=0; i<result.size(); i++) {
          Tuple2<String, TimedPoint> tp = result.get(i);
//          byte[] gridHilbertIndex = LongUtils.LongToBytes(Integer.valueOf(tp._1));
//          byte[] inGridIndex = LongUtils.LongToBytes(i);
          byte[] gridHilbertIndex = IntegerUtils.IntegerToBytes(Integer.valueOf(tp._1));
          byte[] inGridIndex = IntegerUtils.IntegerToBytes(i);
          byte[] index = Bytes.concat(gridHilbertIndex, inGridIndex);
          long l = LongUtils.bytesToLong(index);
//          BigInteger l = new BigInteger(index);
          TimedPoint p = new TimedPoint(tp._2);
          p.setFid(String.valueOf(l));
          tp = new Tuple2<>(tp._1, p);
          output.add(i, tp);
        }
        return output.iterator();
      }
    });

    JavaRDD<Record> records = trajectoryPointLayer.map(new Function<Tuple2<String, TimedPoint>, Record>() {
      @Override
      public Record call(Tuple2<String, TimedPoint> in) throws Exception {
        Long index = Long.valueOf(in._2.getFid());
        Long time = in._2.getTimestamp();
        Double x = in._2.getGeometry().getX();
        Double y = in._2.getGeometry().getY();
        return new Record(index, x, y, time);
      }
    });

    records.cache();
    RecordStats stats = records.map(new Function<Record, RecordStats>() {
      @Override
      public RecordStats call(Record record) throws Exception {
        return new RecordStats(1L, record.index, record.index, record.x, record.y, record.x, record.y, record.time, record.time);
      }
    }).reduce(new Function2<RecordStats, RecordStats, RecordStats>() {
      @Override
      public RecordStats call(RecordStats in1, RecordStats in2) throws Exception {
        Long count = in1.count + in2.count;
        Long indexMin = Math.min(in1.indexMin, in2.indexMin);
        Long indexMax = Math.max(in1.indexMax, in2.indexMax);
        Double xmin = Math.min(in1.xmin, in2.xmin);
        Double xmax = Math.max(in1.xmax, in2.xmax);
        Double ymin = Math.min(in1.ymin, in2.ymin);
        Double ymax = Math.max(in1.ymax, in2.ymax);
        Long timemin = Math.min(in1.timeMin, in2.timeMin);
        Long timemax = Math.max(in1.timeMax, in2.timeMax);
        return new RecordStats(count, indexMin, indexMax,xmin, ymin, xmax, ymax, timemin, timemax);
      }
    });

    JavaRDD<TrainingRecord> trainingRecords = records.map(new Function<Record, TrainingRecord>() {
      @Override
      public TrainingRecord call(Record record) throws Exception {
        Long index = record.index-stats.indexMin;
        Double scaledIndex = (Double.valueOf(index))/stats.indexDiff();
        Double x = (record.x-stats.xmin)/stats.xDiff();
        Double y = (record.y-stats.ymin)/stats.yDiff();
        Double time = (Double.valueOf(record.time-stats.timeMin))/stats.timeDiff();
        return new TrainingRecord(index, scaledIndex, x, y, time);
      }
    });

    System.out.println("Count = " + stats.count);
    System.out.println("MaxIndex = " + stats.indexDiff());

    Dataset<Row> rows = ss.createDataFrame(trainingRecords, TrainingRecord.class);
    rows.write().format("csv").option("delimiter", ",").save(TDRIVE_OUT_DIR);

    ss.stop();
    ss.close();
  }

  @Getter
  @Setter
  @AllArgsConstructor
  public static class TrainingRecord implements Serializable {
    private Long index;
    private Double scaledIndex;
    private Double scaledX;
    private Double scaledY;
    private Double scaledTime;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  public static class Record implements Serializable {
    private Long index;
    private Double x;
    private Double y;
    private Long time;
  }

  @Getter
  @Setter
  public static class RecordStats implements Serializable {
    private Long count;
    private Long indexMin;
    private Long indexMax;
    private Double xmin;
    private Double ymin;
    private Double xmax;
    private Double ymax;
    private Long timeMin;
    private Long timeMax;

    public RecordStats(Long count, Long indexMin, Long indexMax, Double xmin, Double ymin, Double xmax, Double ymax, Long timeMin, Long timeMax) {
      this.count = count;
      this.indexMin = indexMin;
      this.indexMax = indexMax;
      this.xmin = xmin;
      this.ymin = ymin;
      this.xmax = xmax;
      this.ymax = ymax;
      this.timeMin = timeMin;
      this.timeMax = timeMax;
    }

    public Double xDiff() { return xmax - xmin; }
    public Double yDiff() { return ymax - ymin; }
    public Long timeDiff() { return timeMax - timeMin; }
    public Long indexDiff() { return indexMax - indexMin; }
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
