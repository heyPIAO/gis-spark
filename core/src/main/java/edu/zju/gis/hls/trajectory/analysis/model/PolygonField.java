package edu.zju.gis.hls.trajectory.analysis.model;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.SHAPE_FIELD;

/**
 * @author Hu
 * @date 2020/10/3
 **/
public class PolygonField extends Field {

  public PolygonField() {
    super(SHAPE_FIELD.name(), org.locationtech.jts.geom.Polygon.class.getName(), FieldType.SHAPE_FIELD);
  }

}
