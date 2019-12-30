package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import lombok.Getter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Hu
 * @date 2019/12/30
 **/
public class RTree {

  private org.locationtech.jts.index.SpatialIndex si;

  @Getter
  private int count;

  @Getter
  private LayerType layerType;


  public RTree(LayerType layerType) {
    this.layerType = layerType;
    this.count = 0;
    si = new STRtree();
  }

  public <V extends Feature> void insert(Envelope e, V o)  {
    this.si.insert(e, o);
    count ++;
  }

  public <V extends Feature> List<V> query(Envelope e) {
    return (List<V>) this.si.query(e);
  }

  public <V extends Feature> boolean remove(Envelope e, V o) {
    count --;
    return this.si.remove(e, o);
  }

}
