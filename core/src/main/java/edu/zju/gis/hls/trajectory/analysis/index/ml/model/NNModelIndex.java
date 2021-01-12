package edu.zju.gis.hls.trajectory.analysis.index.ml.model;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.*;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2021/1/4
 * 目前只支持被检索对象为点
 * TODO 实现被检索对象为 Polyline 与 Polygon 的检索
 **/
@Getter
@Setter
public class NNModelIndex implements Serializable {

  public static String DAUM_KEY = "DAUM";

  private NNModel model;
  private int[] offset = new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE}; // 偏差，min，max
  private Tuple2<String, Feature>[] features; // 所有 Feature
  private int[] positions; // 排好序的feature的所有点及其对应位置的feature
  private Envelope e; // 数据空间四至，用于查询参数归一化
  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS();
  private boolean isTemporal;
  private long[] timeThreshold = new long[]{-1L, -1L}; // 时间戳的阈值
  private boolean isFinish = false;

  public NNModelIndex(boolean isTemporal) {
    if(isTemporal) this.model = new NNModel(3,1);
    else this.model = new NNModel(2, 1);

    this.isTemporal = isTemporal;
  }

  public NNModelIndex() {
    this(false);
  }

  /**
   * 初始化模型
   * 排序 + 构建 positions 和 features 之间的映射
   * TODO 支持 Polygon 和 LineString，需要修改 Feature 转 Point 部分
   * TODO 排序过程可优化
   * TODO 排序方式的不同对于查询效率的影响
   */
  public void init() {
    List<Tuple2<String, Feature>> l = Arrays.asList(features);
    for (int i=0; i<l.size(); i++) {
      Tuple2<String, Feature> t = new Tuple2<String, Feature>(String.valueOf(i), l.get(i)._2);
      l.set(i, t);
    }
    if (!isTemporal) {
      List<Tuple2<String, Point>> f = l.stream().map(new Function<Tuple2<String, Feature>, Tuple2<String, Point>>() {
        @Override
        public Tuple2<String, Point> apply(Tuple2<String, Feature> in) {
          return new Tuple2<>(in._1, (Point) in._2.getGeometry());
        }
      }).sorted(new Comparator<Tuple2<String, Point>>() {
        @Override
        public int compare(Tuple2<String, Point> o1, Tuple2<String, Point> o2) {
          return o1._2.getX() > o2._2.getX() ? 1 : 0;
        }
      }).collect(Collectors.toList());

      this.positions = new int[f.size()];
      for (int i=0; i<f.size(); i++) {
        positions[i] = Integer.valueOf(f.get(i)._1);
      }
    } else {
      List<Tuple2<String, TemporalPoint>> f = l.stream().map(new Function<Tuple2<String, Feature>, Tuple2<String, TemporalPoint>>() {
        @Override
        public Tuple2<String, TemporalPoint> apply(Tuple2<String, Feature> in) {
          return new Tuple2<>(in._1, (TemporalPoint) in._2.getGeometry());
        }
      }).sorted(new Comparator<Tuple2<String, TemporalPoint>>() {
        @Override
        public int compare(Tuple2<String, TemporalPoint> o1, Tuple2<String, TemporalPoint> o2) {
          return o1._2.getInstant() > o2._2.getInstant() ? 1 : 0;
        }
      }).collect(Collectors.toList());

      this.positions = new int[f.size()];
      for (int i=0; i<f.size(); i++) {
        positions[i] = Integer.valueOf(f.get(i)._1);
      }
    }
  }

  public void build() {
    if (!this.isFinish) {
      init();
      INDArray[] trainingData = mapToTrainningData();
      DataSet ds = new DataSet(trainingData[0], trainingData[1]);
      this.model.train(ds);
      this.generateOffsets(ds);
      this.isFinish = true;
    }
  }

  public void setData(Tuple2<String, Feature>[] features) { this.features = features; }
  public void setData(List<Tuple2<String, Feature>> featureList) {
    int size = featureList.size();
    this.features = new Tuple2[size];
    featureList.toArray(this.features);
  }

  private void generateOffsets(DataSet ds) {
    INDArray predicts = this.model.output(ds.getFeatures()).mul(this.positions.length);
    for (int i=0; i<this.positions.length; i++) {
      int roundPosition = Double.valueOf(predicts.getColumn(0).getDouble(i)).intValue();
      int interval = i-roundPosition;
      if (interval < offset[0]) offset[0] = interval;
      if (interval > offset[1]) offset[1] = interval;
    }
  }

  /**
   * 将数据映射到训练集
   * @return
   */
  private INDArray[] mapToTrainningData() {
    int dimension = this.isTemporal() ? 3:2;
    double[][] inputs = new double[positions.length][dimension];
    double[][] labels = new double[positions.length][1];
    for (int i=0; i<positions.length;i++) {
      Geometry g = features[positions[i]]._2.getGeometry();
      if (g instanceof Point) {
        Point p = (Point) g;
        double x = (p.getX()-e.getMinX())/e.getWidth();
        double y = (p.getY()-e.getMinY())/e.getHeight();
        double[] feature = new double[dimension];
        feature[0] = x;
        feature[1] = y;
        if (g instanceof TemporalPoint && this.isTemporal) {
          TemporalPoint tp = (TemporalPoint) p;
          double t = tp.getInstant();
          feature[2] = t;
        }
        inputs[i] = feature;
        labels[i] = new double[]{ i*1.0/positions.length };
      } else {
        throw new GISSparkException("Unsupport Geometry Type: " + g.getGeometryType());
      }
    }
    INDArray Iinputs = Nd4j.create(inputs);
    INDArray Ilabels = Nd4j.create(labels);
    return new INDArray[]{Iinputs, Ilabels};
  }

  public List<Tuple2<String, Feature>> query(Geometry g) {
    if (!this.isFinish) throw new GISSparkException("Index has not been build");
    if (g instanceof Point || g instanceof LineString) throw new GISSparkException("Unsupport geometry type: " + g.getGeometryType());
    if (g.contains(JTS.toGeometry(this.e))) return Arrays.asList(this.features);
    List<Tuple2<String, Feature>> result = new ArrayList<>();
    Polygon p = (Polygon) g;
    Envelope e = p.getEnvelopeInternal();

    // 左下到右上
    Point high = Term.GEOMETRY_FACTORY.createPoint(new Coordinate(e.getMinX(), e.getMinY()));
    Point low = Term.GEOMETRY_FACTORY.createPoint(new Coordinate(e.getMaxX(), e.getMaxY()));
    int[] highPos = this.getPositionThreshold(high);
    int[] lowPos = this.getPositionThreshold(low);
    int[] predictedPositions = new int[2];
    predictedPositions[0] = Math.min(highPos[0], lowPos[0]);
    predictedPositions[1] = Math.max(highPos[1], lowPos[1]);
    for (int i=predictedPositions[0]; i<=predictedPositions[1]; i++) {
      int pi = this.positions[i];
      Feature f = features[pi]._2;
      if (g.intersects(f.getGeometry())) result.add(features[i]);
    }
    return result;
  }

  /**
   * Hint: DaumNode 的 Geometry 为 CRS 的 Envelope
   * @param
   * @return
   */
  private Tuple2<String, Feature>  generateDaumNode() {
    return new Tuple2<String, Feature>(Term.DAUM_KEY,
      new edu.zju.gis.hls.trajectory.analysis.model.Polygon(GeometryUtil.envelopeToPolygon(CrsUtils.getCrsEnvelope(this.crs))));
  }

  private int[] getPositionThreshold(Point g) {
    INDArray input = null;
    if (this.isTemporal()) {
      TemporalPoint p = (TemporalPoint) g;
      double x = (p.getX()-e.getMinX())/e.getWidth();
      double y = (p.getY()-e.getMinY())/e.getHeight();
      double t = (p.getInstant()*1.0-this.timeThreshold[0])/(this.timeThreshold[1] - this.timeThreshold[0]);
      double[][] inputs = new double[][]{new double[]{x, y, t}};
      input = Nd4j.create(inputs);
    } else {
      double x = (g.getX()-e.getMinX())/e.getWidth();
      double y = (g.getY()-e.getMinY())/e.getHeight();
      double[][] inputs = new double[][]{new double[]{x, y}};
      input = Nd4j.create(inputs);
    }
    int roundPosition = Double.valueOf(this.model.output(input).getColumn(0).getDouble(0) * this.positions.length).intValue();
    int min = roundPosition + offset[0];
    int max = roundPosition + offset[1];
    if (min < 0) min = 0;
    if (max < 0) max = 0;
    if (min > positions.length-1) min = positions.length-1;
    if (max > positions.length-1) max = positions.length-1;
    return new int[]{min, max};
  }

}
