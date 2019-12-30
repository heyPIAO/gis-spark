package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.EnhancedPrecisionOp;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/16
 * 构建四叉树索引的图层
 **/
@Getter
@Setter
public class QuadTreeIndex implements SpatialIndex, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(QuadTreeIndex.class);

  private int z;

  public QuadTreeIndex() {
    this.z = Term.QUADTREE_DEFAULT_LEVEL;
  }

  public QuadTreeIndex(int z) {
    this.z = z;
  }

  /**
   * 构建四叉树索引
   * @param layer
   * @return
   */
  @Override
  public <L extends Layer, T extends IndexedLayer<L>> T index(L layer) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    PyramidConfig pc = new PyramidConfig.PyramidConfigBuilder().setCrs(crs).setzLevelRange(Term.QUADTREE_MIN_Z, Term.QUADTREE_MAX_Z).setBaseMapEnv(CrsUtils.getCrsEnvelope(crs)).build();
    QuadTreeIndexBuiler builder = new QuadTreeIndexBuiler(pc, z);
    QuadTreeIndexLayer result = new QuadTreeIndexLayer(pc, z);
    result.setLayer(layer.flatMapToLayer(builder));
    return (T) result;
  }

  /**
   * 四叉树索引构建器
   *
   * @param <K>
   * @param <V>
   */
  @Getter
  @Setter
  private class QuadTreeIndexBuiler<K, V extends Feature> implements FlatMapFunction<Tuple2<K, V>, Tuple2<String, V>> {

    private PyramidConfig pc;
    private int qz;

    public QuadTreeIndexBuiler(PyramidConfig pc, int z) {
      this.pc = pc;
      this.qz = z;
    }

    @Override
    public Iterator<Tuple2<String, V>> call(Tuple2<K, V> in) throws Exception {
      List<Tuple2<String, V>> result = new ArrayList<>();
      Geometry geom = in._2.getGeometry();
      ReferencedEnvelope envelope = JTS.toEnvelope(geom);
      int zmin = pc.getZLevelRange()[0];
      int zmax = pc.getZLevelRange()[1];
      int z = Math.min(Math.max(zmin, qz), zmax);
      ZLevelInfo tZLevelInfo = GridUtil.initZLevelInfoPZ(pc, envelope)[z - zmin];
      int tx_min = tZLevelInfo.getTileRanges()[0];
      int tx_max = tZLevelInfo.getTileRanges()[1];
      int ty_min = tZLevelInfo.getTileRanges()[2];
      int ty_max = tZLevelInfo.getTileRanges()[3];
      for (int tile_x = tx_min; tile_x <= tx_max; tile_x++) {
        for (int tile_y = ty_min; tile_y <= ty_max; tile_y++) {
          GridID gridID = new GridID();
          gridID.setX(tile_x);
          gridID.setY(tile_y);
          gridID.setzLevel(z);
          Envelope tileEnvelope = GridUtil.createTileBox(gridID, pc);
          V f = (V) in._2.getSelfCopyObject();
          // 得到在每一个瓦片中对应的 geometry
          Geometry tileGeometry = JTS.toGeometry(tileEnvelope);
          Geometry finalGeom = EnhancedPrecisionOp.intersection(geom, tileGeometry);
          f.setGeometry(finalGeom);
          result.add(new Tuple2<>(gridID.toString(), f));
        }
      }
      return result.iterator();
    }
  }
}
