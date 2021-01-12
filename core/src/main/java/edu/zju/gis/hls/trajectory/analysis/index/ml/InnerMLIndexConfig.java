package edu.zju.gis.hls.trajectory.analysis.index.ml;


import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModel;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import lombok.Setter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2021/1/4
 **/
@Getter
@Setter
public class InnerMLIndexConfig  extends IndexConfig implements Serializable {

  private NNModel model;
  private boolean isTemporal = false;
  private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;

  public InnerMLIndexConfig() {
    this.model = new NNModel();
  }

  public InnerMLIndexConfig(NNModel model) {
    this.model = model;
  }

}
