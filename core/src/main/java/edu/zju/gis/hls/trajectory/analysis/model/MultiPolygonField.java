package edu.zju.gis.hls.trajectory.analysis.model;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.SHAPE_FIELD;

/**
 * @author Hu
 * @date 2020/10/3
 **/
public class MultiPolygonField extends Field {

  public MultiPolygonField() {
    super(SHAPE_FIELD.name(), org.locationtech.jts.geom.MultiPolygon.class.getName(), FieldType.SHAPE_FIELD);
  }

}
