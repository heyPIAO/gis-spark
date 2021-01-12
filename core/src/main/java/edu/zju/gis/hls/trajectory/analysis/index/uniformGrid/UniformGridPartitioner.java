package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpaceSplitSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import lombok.Getter;
import lombok.ToString;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/12/10
 **/
@Getter
@ToString(callSuper = true)
public class UniformGridPartitioner extends SpaceSplitSpatialPartitioner {

  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;
  private Envelope extent = CrsUtils.getCrsEnvelope(this.crs); // 空间窗口
  private UniformGridConfig conf;
  private double[] resolution;

  public UniformGridPartitioner(int partitionNum) {
    super(partitionNum);
    this.conf = new UniformGridConfig(findLevelNum(Double.valueOf(Math.sqrt(partitionNum)).intValue() + 1), 2);
    this.isClip = this.conf.isClip();
  }

  public void setCrs(CoordinateReferenceSystem crs) {
    this.crs = crs;
    this.extent = CrsUtils.getCrsEnvelope(this.crs);
    this.resolution = new double[2];
    this.resolution[0] = this.extent.getWidth()/this.conf.getEncoder().getColNum();
    this.resolution[1] = this.extent.getHeight()/this.conf.getEncoder().getColNum();
  }

  public void setConf(UniformGridConfig conf) {
    this.conf = conf;
    this.isClip = conf.isClip();
    // update resolution
    this.resolution[0] = this.extent.getWidth()/this.conf.getEncoder().getColNum();
    this.resolution[1] = this.extent.getHeight()/this.conf.getEncoder().getColNum();
  }

  @Override
  public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
    List<KeyRangeFeature> result = new ArrayList<>();
    if (geometry instanceof Point) {
      Point p = (Point) geometry;
      int x = UniformGridUtil.getGridNum(p.getX(), extent.getMinX(), this.resolution[0]);
      int y = UniformGridUtil.getReverseGridNum(p.getY(), extent.getMaxY(), this.resolution[1]);
      result.add(getKeyRangeFeature(String.valueOf(this.conf.getEncoder().encode(x, y))));
    } else {
      Envelope e = geometry.getEnvelopeInternal();
      // 处理 x
      int xmin = UniformGridUtil.getGridNum(e.getMinX(), extent.getMinX(), this.resolution[0]);
      int xmax = UniformGridUtil.getGridNum(e.getMaxX(), extent.getMinX(), this.resolution[0]);
      // 处理 y
      int ymax = UniformGridUtil.getReverseGridNum(e.getMinY(), extent.getMaxY(), this.resolution[1]);
      int ymin = UniformGridUtil.getReverseGridNum(e.getMaxY(), extent.getMaxY(), this.resolution[1]);
      for (int x=xmin; x<=xmax; x++) {
        for (int y=ymin; y<=ymax; y++) {
          KeyRangeFeature feature = getKeyRangeFeature(String.valueOf(this.conf.getEncoder().encode(x, y)));
          if (feature.getGeometry().intersects(geometry)) {
            result.add(getKeyRangeFeature(String.valueOf(this.conf.getEncoder().encode(x, y))));
          }
        }
      }
    }
    return result;
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (this.keyRanges!= null && this.keyRanges.size() > 0 && this.keyRanges.get(key)!=null) {
      return this.keyRanges.get(key);
    }
    BigInteger grid = BigInteger.valueOf(Long.valueOf(key));
    long[] gridNum = this.conf.getEncoder().decode(grid);
    long x = gridNum[0];
    long y = gridNum[1];
    Polygon gridEnvelope = JTS.toGeometry(this.getGridEnvelope(x, y));
    return new KeyRangeFeature(key,gridEnvelope, getPartition(key));
  }

  private Envelope getGridEnvelope(long x, long y) {
    double xmin = x * resolution[0] + this.extent.getMinX();
    double xmax = (x+1) * resolution[0] + this.extent.getMinX();
    double ymin = this.extent.getMaxY() - (y+1) * resolution[1];
    double ymax = this.extent.getMaxY() - y * resolution[1];
    return new Envelope(xmin, xmax, ymin, ymax);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UniformGridPartitioner)) return false;
    UniformGridPartitioner p = (UniformGridPartitioner) o;
    boolean flag = this.crs.equals(p.crs);
    flag = flag && this.conf.equals(p.conf);
    return flag;
  }

  private static int findLevelNum(int gridNum) {
    return Double.valueOf(Math.sqrt(gridNum)).intValue() + 1;
  }

}
