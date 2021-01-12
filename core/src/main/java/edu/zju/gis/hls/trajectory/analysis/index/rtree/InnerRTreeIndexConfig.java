package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import lombok.Setter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2020/6/23
 **/
@Getter
@Setter
public class InnerRTreeIndexConfig extends IndexConfig implements Serializable {

  private int nodeCapacity;
  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;

  public InnerRTreeIndexConfig() {
    this(100);
  }

  public InnerRTreeIndexConfig(int nodeCapacity) {
    this.nodeCapacity = nodeCapacity;
  }

  public InnerRTreeIndexConfig(int nodeCapacity, CoordinateReferenceSystem crs) {
    this.nodeCapacity = nodeCapacity;
    this.crs = crs;
  }

}
