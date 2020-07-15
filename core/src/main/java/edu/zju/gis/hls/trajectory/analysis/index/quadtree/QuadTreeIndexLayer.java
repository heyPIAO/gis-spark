package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/18
 * 四叉树索引的 RDD Layer
 **/
@Getter
@Setter
@Slf4j
public class QuadTreeIndexLayer<L extends Layer> extends KeyIndexedLayer<L> {

  private PyramidConfig pc;
  private QuadTreeIndexConfig conf;

  public QuadTreeIndexLayer(PyramidConfig pc, QuadTreeIndexConfig conf) {
    this.indexType = IndexType.QUADTREE;
    this.pc = pc;
    this.conf = conf;
  }

  @Override
  public L query(Geometry geometry) {

    List<String> tiles = this.queryPartitionsIds(geometry);

    return (L) this.layer.filterToLayer(new Function<Tuple2, Boolean>() {
      @Override
      public Boolean call(Tuple2 in) throws Exception {
        Object k = in._1;
        if (k instanceof String && tiles.contains(k)) {
          Feature f = (Feature) in._2;
          return f.getGeometry().intersects(geometry);
        }
        return false;
      }
    });
  }

  public List<String> queryPartitionsIds(Geometry geometry) {
    ReferencedEnvelope envelope = JTS.toEnvelope(geometry);
    ZLevelInfo tZLevelInfo = GridUtil.initZLevelInfoPZ(pc, envelope)[conf.getIndexLevel() - pc.getZLevelRange()[0]];
    List<String> tiles = new ArrayList<>();
    for (int tile_x = tZLevelInfo.getTileRanges()[0]; tile_x <= tZLevelInfo.getTileRanges()[1]; tile_x++) {
      for (int tile_y = tZLevelInfo.getTileRanges()[2]; tile_y <= tZLevelInfo.getTileRanges()[3]; tile_y++) {
        tiles.add((new GridID(conf.getIndexLevel(), tile_x, tile_y)).toString());
      }
    }
    return tiles;
  }

}
