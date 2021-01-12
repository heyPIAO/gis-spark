package edu.zju.gis.hls.trajectory.doc.preprocess;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.uniformGrid.UniformGridConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.doc.model.Record;
import edu.zju.gis.hls.trajectory.doc.model.RecordStats;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord3;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.math.BigInteger;
import java.util.*;

/**
 * @author Hu
 * @date 2020/12/8
 * 微软轨迹数据转为测试点数据
 * HINT：原始数据按点存储，根据训练需要将数据排序并归一化，并将其写出到文件
 **/
public class H2PointTrainData {

  private static String TDRIVE_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\test_10_wkt";
  private static String TDRIVE_OUT_DIR = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\temporalPoint\\single_dim_y";
  private static String ORDER_MODE = "SINGLE_DIM"; // MULTI_DIM: 三个维度独立排序；
  // SINGLE_DIM: 按Flood（x,y组成bucket后按t排序，bucket按先x后y排序）规则排序
//  private static int DATA_DIMENSION = 3; // 用于构建索引的数据维度，2或3
  private static int RESOLUTION_X = 1; // 对于 SINGLE_DIM 索引，在x方向上构建bucket的分辨率，1代表小数点后1位
  private static int RESOLUTION_Y = 1; // 对于 SINGLE_DIM 索引，在y方向上构建bucket的分辨率，1代表小数点后1位
  private static int ORDER_DIM = 1; // 对于 SINGLE_DIM 索引，0表示先按x排序，1表示先按y排序

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

    trajectoryPointLayer.makeSureCached();

    // 获得每个partition内部每个格网内的数据量
    JavaPairRDD<BigInteger, Long> countsInGridRDD = trajectoryPointLayer.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, BigInteger, Long>() {
      @Override
      public Iterator<Tuple2<BigInteger, Long>> call(Iterator<Tuple2<String, TimedPoint>> in) throws Exception {
        Map<String, Long> resultMap = new HashMap<>();
        while (in.hasNext()) {
          Tuple2<String, TimedPoint> i = in.next();
          if (resultMap.keySet().contains(i._1)) resultMap.replace(i._1, resultMap.get(i._1) + 1);
          else resultMap.put(i._1, 1L);
        }
        List<Tuple2<BigInteger, Long>> result = new ArrayList<>();
        for (String key: resultMap.keySet()) {
          result.add(new Tuple2<>(new BigInteger(key), resultMap.get(key)));
        }
        return result.iterator();
      }
    });

    List<Tuple2<BigInteger, Long>> countsInGrid = new ArrayList<>(countsInGridRDD.collect());
    Collections.sort(countsInGrid, (o1, o2) -> o1._1.compareTo(o2._1));

    // 计算每个格网内部数据编号
    Map<String, Tuple3<Integer, Long, Long>> orderedGridMap = new HashMap<>();
    Long tt = 0L;
    for (int i=0; i<countsInGrid.size(); i++) {
      Tuple2<BigInteger, Long> t = countsInGrid.get(i);
      orderedGridMap.put(String.valueOf(t._1), new Tuple3<>(i, t._2, tt));
      tt += t._2;
    }

    JavaRDD<Record> records = trajectoryPointLayer.map(new Function<Tuple2<String, TimedPoint>, Record>() {
      @Override
      public Record call(Tuple2<String, TimedPoint> in) throws Exception {
        Long time = in._2.getTimestamp();
        Double x = in._2.getGeometry().getX();
        Double y = in._2.getGeometry().getY();
        return new Record(x, y, time);
      }
    });

    // 计算图层元数据信息，包括总数，四至，与时间范围
    records.cache();
    RecordStats stats = records.map(new Function<Record, RecordStats>() {
      @Override
      public RecordStats call(Record record) throws Exception {
        return new RecordStats(1L, record.getX(), record.getY(), record.getX(), record.getY(), record.getTime(), record.getTime());
      }
    }).reduce(new Function2<RecordStats, RecordStats, RecordStats>() {
      @Override
      public RecordStats call(RecordStats in1, RecordStats in2) throws Exception {
        Long count = in1.getCount() + in2.getCount();
        Double xmin = Math.min(in1.getXmin(), in2.getXmin());
        Double xmax = Math.max(in1.getXmax(), in2.getXmax());
        Double ymin = Math.min(in1.getYmin(), in2.getYmin());
        Double ymax = Math.max(in1.getYmax(), in2.getYmax());
        Long timemin = Math.min(in1.getTimeMin(), in2.getTimeMin());
        Long timemax = Math.max(in1.getTimeMax(), in2.getTimeMax());
        return new RecordStats(count, xmin, ymin, xmax, ymax, timemin, timemax);
      }
    });

    if (ORDER_MODE.equals("SINGLE_DIM")) {
      trajectoryPointLayer = (TrajectoryPointLayer) trajectoryPointLayer.mapPartitionsToLayer(new PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, String, TimedPoint>() {
        @Override
        public Iterator<Tuple2<String, TimedPoint>> call(Iterator<Tuple2<String, TimedPoint>> in) throws Exception {
          // 格网内部排序
          if (!in.hasNext()) return in;
          List<Tuple2<String, TimedPoint>> inl = IteratorUtils.toList(in);
          Map<String, List<Tuple2<String,TimedPoint>>> outputMap = new HashMap<>();
          for (Tuple2<String, TimedPoint> t: inl) {
            if (outputMap.keySet().contains(t._1)) {
              outputMap.get(t._1).add(t);
            } else {
              List<Tuple2<String, TimedPoint>> r = new ArrayList<>();
              r.add(t);
              outputMap.put(t._1, r);
            }
          }

          for (String key: outputMap.keySet()) {
            List<Tuple2<String, TimedPoint>> ts = outputMap.get(key);
            List<Tuple2<String, TimedPoint>> tsu = new ArrayList<>();
            Collections.sort(ts, new Comparator<Tuple2<String, TimedPoint>>() {
              @Override
              public int compare(Tuple2<String, TimedPoint> o1, Tuple2<String, TimedPoint> o2) {
                Double x1 = o1._2.getGeometry().getX();
                Double y1 = o1._2.getGeometry().getY();
                Long t1 = o1._2.getGeometry().getInstant();
                Double x2 = o2._2.getGeometry().getX();
                Double y2 = o2._2.getGeometry().getY();
                Long t2 = o2._2.getGeometry().getInstant();
                if (ORDER_DIM == 0) {
                  // 先按 x 排序
                  int x = compareWithPrecision(x1, x2, RESOLUTION_X);
                  if (x != 0) return x;
                  // x 相同，按 y 排序
                  int y = compareWithPrecision(y1, y2, RESOLUTION_Y);
                  if (y != 0) return y;
                } else if (ORDER_DIM == 1) {
                  // 先按 y 排序
                  int y = compareWithPrecision(y1, y2, RESOLUTION_Y);
                  if (y != 0) return y;
                  // y 相同，按 x 排序
                  int x = compareWithPrecision(x1, x2, RESOLUTION_X);
                  if (x != 0) return x;
                } else {
                  throw new GISSparkException("Unsupport ORDER_DIM: " + ORDER_DIM);
                }
                // x, y 相同，按t排序
                return Long.compare(t1, t2);
              }
            });

            long startIndex = orderedGridMap.get(ts.get(0)._1)._3();

            // 计算每个点的全局顺序
            for (int i=0; i<ts.size(); i++) {
              Tuple2<String, TimedPoint> tp = ts.get(i);
              long index = startIndex + i;
              TimedPoint p = new TimedPoint(tp._2);
              p.setFid(String.valueOf(index));
              tp = new Tuple2<>(tp._1, p);
              tsu.add(i, tp);
            }

            outputMap.replace(key, tsu);
          }

          List<Tuple2<String, TimedPoint>> results = new ArrayList<>();
          for (String key: outputMap.keySet()) {
            if (results.size() == 0) results = new ArrayList<>(outputMap.get(key));
            else results.addAll(outputMap.get(key));
          }

          return results.iterator();
        }
      });

      // 将x,y,t与输出的序列号归一化，得到训练数据
      JavaRDD<TrainingRecord> trainingRecords = trajectoryPointLayer.map(new Function<Tuple2<String, TimedPoint>, TrainingRecord>() {
        @Override
        public TrainingRecord call(Tuple2<String, TimedPoint> record) throws Exception {
          Long index = Long.valueOf(record._2.getFid());
          Double scaledIndex = (Double.valueOf(index))/stats.getCount();
          Double scaledX = (record._2.getGeometry().getX()-stats.getXmin())/stats.xDiff();
          Double scaledY = (record._2.getGeometry().getY()-stats.getYmin())/stats.yDiff();
          Double scaledTime = (Double.valueOf(record._2.getGeometry().getInstant()-stats.getTimeMin()))/stats.timeDiff();
          return new TrainingRecord(index, record._2.getGeometry().getX(), record._2.getGeometry().getY(), record._2.getGeometry().getInstant(), scaledX, scaledY, scaledTime, scaledIndex, String.valueOf(record._2.getAttribute("trajId")));
        }
      });

      // 写出训练数据
      trainingRecords.repartition(1).saveAsTextFile(TDRIVE_OUT_DIR);
    } else if (ORDER_MODE.equals("MULTI_DIM")) {
      // 三个维度单独排序，输出label为三元组
      JavaPairRDD<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>> labels = trajectoryPointLayer.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, TimedPoint>>, String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>() {
        @Override
        public Iterator<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>> call(Iterator<Tuple2<String, TimedPoint>> in) throws Exception {
          Map<String, List<Tuple2<String, TimedPoint>>> inMap = new HashMap<>();
          Map<String, List<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>>> outputMap = new HashMap<>();
          if (!in.hasNext()) return (new ArrayList<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>>()).iterator();
          List<Tuple2<String, TimedPoint>> inl = IteratorUtils.toList(in);
          // 先按格网号分离数据
          for (Tuple2<String, TimedPoint> t: inl) {
            if (inMap.keySet().contains(t._1)) {
              inMap.get(t._1).add(t);
            } else {
              List<Tuple2<String, TimedPoint>> r = new ArrayList<>();
              r.add(t);
              inMap.put(t._1, r);
            }
          }

          for (String key: inMap.keySet()) {
            List<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>> output = new ArrayList<>();
            List<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>> temp = new ArrayList<>();
            List<Tuple2<String, TimedPoint>> ts = inMap.get(key);
            long startIndex = orderedGridMap.get(ts.get(0)._1)._3();
            // 按规则排序
            // 按x排序，得到x维度序列
            ts.sort((o1, o2) -> Double.compare(o1._2.getGeometry().getX(), o2._2.getGeometry().getX()));
            for (int i=0; i<ts.size(); i++) {
              Tuple2<String, TimedPoint> t = ts.get(i);
              long index = startIndex + i;
              Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>> t2
                = new Tuple2<>(t._1, new Tuple2<>(t._2, new Tuple3<>(index, -1L, -1L)));
              output.add(t2);
            }

            ts.clear();
            // 按y排序，得到y维度序列
            output.sort((o1, o2) -> Double.compare(o1._2._1.getGeometry().getY(), o2._2._1.getGeometry().getY()));
            for (int i=0; i<output.size(); i++) {
              Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>> t = output.get(i);
              long index = startIndex + i;
              t = new Tuple2<>(t._1, new Tuple2<>(t._2._1, new Tuple3<>(t._2._2._1(), index, -1L)));
              temp.add(t);
            }

            output.clear();
            // 按t排序，得到z维度序列
            temp.sort((o1, o2) -> Double.compare(o1._2._1.getGeometry().getInstant(), o2._2._1.getGeometry().getInstant()));
            for (int i=0; i<temp.size(); i++) {
              Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>> t = temp.get(i);
              long index = startIndex + i;
              t = new Tuple2<>(t._1, new Tuple2<>(t._2._1, new Tuple3<>(t._2._2._1(), t._2._2._2(), index)));
              output.add(t);
            }

            outputMap.put(key, output);
          }

          List<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>> result = new ArrayList<>();
          for (String key: outputMap.keySet()) {
            if (result.size() == 0) result = new ArrayList<>(outputMap.get(key));
            else result.addAll(outputMap.get(key));
          }

          return result.iterator();
        }
      });

      // 将x,y,t与输出的序列号归一化，得到训练数据
      JavaRDD<TrainingRecord3>  trainingRecord3s = labels.map(new Function<Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>>, TrainingRecord3>() {
        @Override
        public TrainingRecord3 call(Tuple2<String, Tuple2<TimedPoint, Tuple3<Long, Long, Long>>> in) throws Exception {
          long indexX = in._2._2._1();
          double scaledIndexX = indexX * 1.0 / stats.getCount();
          long indexY = in._2._2._2();
          double scaledIndexY = indexY * 1.0 / stats.getCount();
          long indexT = in._2._2._3();
          double scaledIndexT = indexT * 1.0 / stats.getCount();
          double x = in._2._1.getGeometry().getX();
          double scaledX = (x-stats.getXmin())/stats.xDiff();
          double y = in._2._1.getGeometry().getY();
          double scaledY = (y-stats.getYmin())/stats.yDiff();
          long t = in._2._1.getGeometry().getInstant();
          double scaledT = (t-stats.getTimeMin())*1.0/stats.timeDiff();
          return new TrainingRecord3(indexX, indexY, indexT, x, y, t, scaledX, scaledY, scaledT, scaledIndexX, scaledIndexY, scaledIndexT, String.valueOf(in._2._1.getAttribute("trajId")));
        }
      });

      // 写出训练数据
      trainingRecord3s.repartition(1).saveAsTextFile(TDRIVE_OUT_DIR);
    } else {
      throw new GISSparkException("Unsupport order mode: " + ORDER_MODE);
    }

    ss.stop();
    ss.close();
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

  private static int compareWithPrecision(double o1, double o2, int scale) {
    return Integer.compare((int)o1 * scale * 10, (int) o2 * scale * 10);
  }

}
