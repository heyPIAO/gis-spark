package edu.zju.gis.hls.trajectory.analysis.index.unifromGrid;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpaceSplitDistributeSpatialPartitioner;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/8/24
 * 均匀格网的分区器
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class UniformGridPartitioner extends SpaceSplitDistributeSpatialPartitioner {

  private PyramidConfig pc;
  private UniformGridIndexConfig conf;
  private int z;

  public UniformGridPartitioner(PyramidConfig pc, UniformGridIndexConfig conf, int partitionNum) {
    super(partitionNum);
    this.pc = pc;
    this.conf = conf;
    int zmin = pc.getZMin();
    int zmax = pc.getZMax();
    this.z = Math.min(Math.max(zmin, this.conf.getIndexLevel()), zmax);
    this.isClip = conf.isClip();
  }

  public UniformGridPartitioner(PyramidConfig pc, int partitionNum) {
    this(pc, new UniformGridIndexConfig(), partitionNum);
  }

  @Override
  public KeyRangeFeature getKeyRangeFeature(String key) {
    if (this.keyRanges!= null && this.keyRanges.size() > 0 && this.keyRanges.get(key)!=null) {
      return this.keyRanges.get(key);
    }
    GridID gridID = GridID.fromString(key);
    Polygon tileEnvelope = GridUtil.createTileBoxGeo(gridID, pc);
    return new KeyRangeFeature(key, tileEnvelope, getPartition(key));
  }

  @Override
  public List<String> getKey(Geometry geometry) {
    ReferencedEnvelope envelope = JTS.toEnvelope(geometry);
    ZLevelInfo tZLevelInfo = GridUtil.initZLevelInfoPZ(pc, envelope)[conf.getIndexLevel() - pc.getZLevelRange()[0]];
    List<String> keys = new ArrayList<>();
    for (int tile_x = tZLevelInfo.getTileRanges()[0]; tile_x <= tZLevelInfo.getTileRanges()[1]; tile_x++) {
      for (int tile_y = tZLevelInfo.getTileRanges()[2]; tile_y <= tZLevelInfo.getTileRanges()[3]; tile_y++) {
        GridID gridID = new GridID();
        gridID.setX(tile_x);
        gridID.setY(tile_y);
        gridID.setzLevel(z);
        Polygon tileEnvelope = GridUtil.createTileBoxGeo(gridID, pc);
        if (tileEnvelope.intersects(geometry)) {
          keys.add((new GridID(conf.getIndexLevel(), tile_x, tile_y)).toString());
        }
      }
    }
    return keys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UniformGridPartitioner that = (UniformGridPartitioner) o;
    boolean flag = (z == that.z);
    flag = flag && (pc.getBaseMapEnv().equals(that.pc.getBaseMapEnv()));
    return flag;
  }

}
