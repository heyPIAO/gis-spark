package edu.zju.gis.hls.trajectory.analysis.index.ml;

import edu.zju.gis.hls.trajectory.analysis.index.InnerSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModelIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2021/1/4
 **/
@Getter
public class InnerMLIndex implements InnerSpatialIndex, Serializable {

  private InnerMLIndexConfig config;

  public InnerMLIndex() {
    this(new InnerMLIndexConfig());
  }

  public InnerMLIndex(InnerMLIndexConfig config) {
    this.config = config;
  }

  /**
   * 在每个分片内部构建 ML 索引
   * @param layer
   * @return
   */
  @Override
  public InnerMLIndexLayer index(KeyIndexedLayer layer) {
    return this.indexMLModel(layer);
  }

  private InnerMLIndexLayer indexMLModel(KeyIndexedLayer layer) {
    Layer l = layer.getLayer();
    InnerMLIndexLayer result = new InnerMLIndexLayer();
    result.setLayer(layer);
    result.setIndexedPartition(l.mapPartitionsToPair(new InnerMLIndex.InnerMLIndexBuilder(this.config)));
    return result;
  }

  @Getter
  private class InnerMLIndexBuilder implements PairFlatMapFunction<Iterator<Tuple2<String, Feature>>, String, NNModelIndex> {

    private InnerMLIndexConfig conf;

    public InnerMLIndexBuilder(InnerMLIndexConfig conf) {
      this.conf = conf;
    }

    @Override
    public Iterator<Tuple2<String, NNModelIndex>> call(Iterator<Tuple2<String, Feature>> t) throws Exception {
      List<Tuple2<String, NNModelIndex>> result = new ArrayList<>();
      Map<String, List<Tuple2<String, Feature>>> trainingTuples = new HashMap<>();
      Map<String, ModelInitalStats> modelInitalStats = new HashMap<>();

      // 配置模型训练数据
      while(t.hasNext()) {
        Tuple2<String, Feature> m = t.next();
        String gridId = m._1;
        List<Tuple2<String, Feature>> traningTuple = trainingTuples.get(gridId);
        if (traningTuple == null) {
          traningTuple = new LinkedList<>();
          traningTuple.add(m);
          trainingTuples.put(gridId, traningTuple);
        } else {
          traningTuple.add(m);
        }

        ModelInitalStats stats = modelInitalStats.get(gridId);
        if (stats == null) {
          stats = new ModelInitalStats(m._2.getGeometry());
          modelInitalStats.put(gridId, stats);
        } else {
          stats.update(m._2.getGeometry());
        }
      }

      // 训练模型
      for (String key: trainingTuples.keySet()) {
        List<Tuple2<String, Feature>> tuples = trainingTuples.get(key);
        NNModelIndex modelIndex = new NNModelIndex();
        modelIndex.setTemporal(this.conf.isTemporal());
        modelIndex.setCrs(this.conf.getCrs());
        modelIndex.setData(tuples);
        modelIndex.setE(modelInitalStats.get(key).toEnvelope());
        modelIndex.setTimeThreshold(modelInitalStats.get(key).toTimeThreshold());
        modelIndex.build();
        result.add(new Tuple2<>(key, modelIndex));
      }

      return result.iterator();
    }
  }

  @Getter
  @Setter
  private class ModelInitalStats implements Serializable {
    private double minx;
    private double miny;
    private double maxx;
    private double maxy;
    private long mint = -1L;
    private long maxt = -1L;

    public ModelInitalStats(Geometry g) {
      if (g instanceof Point) {
        Point p = (Point) g;
        this.minx = this.maxx = p.getX();
        this.miny = this.maxy = p.getY();
        if (p instanceof TemporalPoint) {
          TemporalPoint tp = (TemporalPoint) p;
          this.mint = this.maxt = tp.getInstant();
        }
      } else {
        Envelope e = g.getEnvelopeInternal();
        this.minx = e.getMinX();
        this.maxx = e.getMaxX();
        this.miny = e.getMinY();
        this.maxy = e.getMaxY();
        if (g instanceof TemporalLineString) {
          TemporalLineString ts = (TemporalLineString) g;
          this.mint = ts.getInstants()[0];
          this.maxt = ts.getInstants()[1];
        }
      }
    }

    public void update(Geometry g) {
      if (g instanceof Point) {
        Point p = (Point) g;
        this.minx = Math.min(this.minx, p.getX());
        this.maxx = Math.max(this.maxx, p.getX());
        this.miny = Math.min(this.miny, p.getY());
        this.maxy = Math.max(this.maxy, p.getY());
        if (p instanceof TemporalPoint) {
          TemporalPoint tp = (TemporalPoint) p;
          this.mint = Math.min(this.mint, tp.getInstant());
          this.maxt = Math.max(this.maxt, tp.getInstant());
        }
      } else {
        Envelope e = g.getEnvelopeInternal();
        this.minx = Math.min(this.minx, e.getMinX());
        this.maxx = Math.max(this.maxx, e.getMaxX());
        this.miny = Math.min(this.miny, e.getMinY());
        this.maxy = Math.max(this.maxy, e.getMaxY());
        if (g instanceof TemporalLineString) {
          TemporalLineString ts = (TemporalLineString) g;
          this.mint = Math.min(this.mint, ts.getInstants()[0]);
          this.maxt = Math.max(this.maxt, ts.getInstants()[1]);
        }
      }
    }

    public Envelope toEnvelope() {
      return new Envelope(minx, maxx, miny, maxy);
    }

    public long[] toTimeThreshold() {
      return new long[]{mint, maxt};
    }
  }

}
