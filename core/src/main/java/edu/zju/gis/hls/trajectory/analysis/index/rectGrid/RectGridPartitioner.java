package edu.zju.gis.hls.trajectory.analysis.index.rectGrid;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpaceSplitSpatialPartitioner;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/8/24
 * 二维均匀格网的分区器
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class RectGridPartitioner extends SpaceSplitSpatialPartitioner {

  private PyramidConfig pc;
  private RectGridIndexConfig conf;
  private int z;

  public RectGridPartitioner(PyramidConfig pc, RectGridIndexConfig conf, int partitionNum) {
    super(partitionNum);
    this.pc = pc;
    this.conf = conf;
    int zmin = pc.getZMin();
    int zmax = pc.getZMax();
    this.z = Math.min(Math.max(zmin, this.conf.getIndexLevel()), zmax);
    this.isClip = conf.isClip();
  }

  public RectGridPartitioner(PyramidConfig pc, int partitionNum) {
    this(pc, new RectGridIndexConfig(), partitionNum);
  }

  @Override
  public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
    return this.getKey(geometry).stream().map(x->this.getKeyRangeFeature(x)).collect(Collectors.toList());
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (this.keyRanges!= null && this.keyRanges.size() > 0 && this.keyRanges.get(key)!=null) {
      return this.keyRanges.get(key);
    }
    Grid grid = Grid.fromString(key);
    Polygon tileEnvelope = RectGridUtil.createTileBoxGeo(grid, pc);
    return new KeyRangeFeature(key, tileEnvelope, getPartition(key));
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    ReferencedEnvelope envelope = JTS.toEnvelope(geometry);
    ZLevelInfo tZLevelInfo = RectGridUtil.initZLevelInfoPZ(pc, envelope)[conf.getIndexLevel() - pc.getZLevelRange()[0]];
    List<String> keys = new ArrayList<>();
    for (int tile_x = tZLevelInfo.getTileRanges()[0]; tile_x <= tZLevelInfo.getTileRanges()[1]; tile_x++) {
      for (int tile_y = tZLevelInfo.getTileRanges()[2]; tile_y <= tZLevelInfo.getTileRanges()[3]; tile_y++) {
        Grid grid = new Grid();
        grid.setX(tile_x);
        grid.setY(tile_y);
        grid.setzLevel(z);
        Polygon tileEnvelope = RectGridUtil.createTileBoxGeo(grid, pc);
        if (tileEnvelope.intersects(geometry)) {
          keys.add((new Grid(conf.getIndexLevel(), tile_x, tile_y)).toString());
        }
      }
    }
    return keys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RectGridPartitioner that = (RectGridPartitioner) o;
    boolean flag = (z == that.z);
    flag = flag && (pc.getBaseMapEnv().equals(that.pc.getBaseMapEnv()));
    return flag;
  }

}
