package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * @author Hu
 * @date 2019/9/21
 **/
@Getter
@Setter
@ToString
public class LayerMetadata extends Feature<Polygon> {

  private String layerId;
  private String layerName;
  private Long layerCount;
  private CoordinateReferenceSystem crs;

  public LayerMetadata(String fid, Polygon geometry, LinkedHashMap<Field, Object> attributes, String layerId, String layerName, CoordinateReferenceSystem crs) {
    super(fid, geometry, attributes);
    this.layerId = layerId;
    this.layerName = layerName;
    this.crs = crs;
  }

  public LayerMetadata() {
    super();
    this.layerId = UUID.randomUUID().toString();
    this.layerName = this.layerId;
    this.crs = Term.DEFAULT_CRS;
  }

  public LayerMetadata(LayerMetadata metadata) {
    super(metadata);
    this.layerId = metadata.layerId;
    this.layerName = metadata.layerName;
    this.crs = metadata.crs;
  }

  public Envelope getExtent() {
    return this.geometry.getEnvelopeInternal();
  }

  public ReferencedEnvelope getReferencedExtent() {
    return new ReferencedEnvelope(this.geometry.getEnvelopeInternal(), crs);
  }

  public Field getIdField() {
    for (Field f: this.getAttributes().keySet()) {
      if (f.getFieldType().equals(FieldType.ID_FIELD)) return f;
    }
    return null;
  }

  public Field getShapeField() {
    for (Field f: this.getAttributes().keySet()) {
      if (f.getFieldType().equals(FieldType.SHAPE_FIELD)) return f;
    }
    return null;
  }

  public void setAttributes(Field[] attributes) {
    LinkedHashMap<Field, Object> a = new LinkedHashMap<>();
    for (Field f: attributes) {
      a.put(f, f.getType());
    }
    super.setAttributes(a);
  }

}
