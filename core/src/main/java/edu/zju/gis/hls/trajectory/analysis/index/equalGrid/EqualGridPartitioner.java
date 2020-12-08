package edu.zju.gis.hls.trajectory.analysis.index.equalGrid;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpaceSplitSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.ToString;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/12/3
 * 一维均匀格网分区器
 **/
@Getter
@ToString(callSuper = true)
public class EqualGridPartitioner extends SpaceSplitSpatialPartitioner {

  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;
  private Envelope extent = CrsUtils.getCrsEnvelope(this.crs);
  private EqualGridIndexConfig conf;

  public EqualGridPartitioner(int partitionNum) {
    super(partitionNum);
    this.conf = new EqualGridIndexConfig(partitionNum);
  }

  public void setCrs(CoordinateReferenceSystem crs) {
    this.crs = crs;
    this.extent = CrsUtils.getCrsEnvelope(this.crs);
  }

  public void setConf(EqualGridIndexConfig conf) {
    this.conf = conf;
    this.isClip = conf.isClip();
  }

  private double getResolution() {
    double total = 0.0;
    switch (conf.getDimension()) {
      case 0: total = extent.getMaxX() - extent.getMinX(); break;
      case 1: total = extent.getMaxY() - extent.getMinY(); break;
      default:
        throw new GISSparkException("Unsupport dimension type for EqualGridPartitioner: " + conf.getDimension());
    }
    return total/conf.getNum();
  }

  private double getMax() {
    double max = 0.0;
    switch (conf.getDimension()) {
      case 0: max = extent.getMaxX(); break;
      case 1: max = extent.getMaxY(); break;
      default:
        throw new GISSparkException("Unsupport dimension type for EqualGridPartitioner: " + conf.getDimension());
    }
    return max;
  }

  private double getMin() {
    double min = 0.0;
    switch (conf.getDimension()) {
      case 0: min = extent.getMinX(); break;
      case 1: min = extent.getMinY(); break;
      default:
        throw new GISSparkException("Unsupport dimension type for EqualGridPartitioner: " + conf.getDimension());
    }
    return min;
  }

  @Override
  public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
    return this.getKey(geometry).stream().map(x->this.getKeyRangeFeature(x)).collect(Collectors.toList());
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    ReferencedEnvelope envelope = JTS.toEnvelope(geometry);
    List<String> keys = new ArrayList<>();
    int min;
    int max;
    switch (conf.getDimension()) {
      case 0:
        min = (int) ((envelope.getMinX() - this.getMin()) / getResolution());
        max = ((int) ((envelope.getMaxX() - this.getMin()) / getResolution())) + 1;
        break;
      case 1:
        min = (int) ((envelope.getMinY() - this.getMin()) / getResolution());
        max = ((int) ((envelope.getMaxY() - this.getMin()) / getResolution())) + 1;
        break;
      default:
        throw new GISSparkException("Unsupport dimension type for EqualGridPartitioner: " + conf.getDimension());
    }
    for (int i=min; i<=max; i++) {
      keys.add(String.valueOf(i));
    }
    return keys;
  }

  /**
   * 根据 key 获取对应分区的空间范围
   * @param key
   * @return
   */
  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (this.keyRanges!= null && this.keyRanges.size() > 0 && this.keyRanges.get(key)!=null) {
      return this.keyRanges.get(key);
    }
    EqualGrid equalGrid = EqualGrid.fromString(key);
    Polygon gridEnvelope = JTS.toGeometry(this.getGridEnvelope(equalGrid));
    return new KeyRangeFeature(key,gridEnvelope, getPartition(key));
  }

  private Envelope getGridEnvelope(EqualGrid grid) {
    double dmin = this.getResolution() * grid.getIndex();
    double dmax = dmin + this.getResolution();
    switch (conf.getDimension()) {
      // 按精度划分格网
      case 0: return new Envelope(dmin, dmax, this.extent.getMinY(), this.extent.getMaxY());
      // 按纬度划分格网
      case 1: return new Envelope(this.extent.getMinX(), this.extent.getMaxX(), dmin, dmax);
      default:
        throw new GISSparkException("Unsupport dimension type for EqualGridPartitioner: " + conf.getDimension());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EqualGridPartitioner that = (EqualGridPartitioner) o;
    boolean flag = (crs == that.crs);
    flag = flag && (conf == that.conf);
    return flag;
  }

}
