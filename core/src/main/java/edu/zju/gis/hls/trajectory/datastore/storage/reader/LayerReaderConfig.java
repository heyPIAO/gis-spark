package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Hu
 * @date 2019/12/19
 **/
@Getter
@Setter
@ToString
@NoArgsConstructor
public abstract class LayerReaderConfig implements Serializable {

  protected String layerId = UUID.randomUUID().toString();
  protected String layerName;
  protected String sourcePath;
  protected LayerType layerType;
  protected Field[] attributes; // 不包括 shape, id, Time 的所有需要读取的 Field 信息
  protected Field shapeField = Term.FIELD_DEFAULT_SHAPE;
  protected Field idField = Term.FIELD_DEFAULT_ID;
  protected Field timeField = Term.FIELD_DEFAULT_TIME;
  protected CoordinateReferenceSystem crs = Term.DEFAULT_CRS;

  public LayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    this.sourcePath = sourcePath;
    this.layerType = layerType;
    this.layerName = layerName;
  }

  public Field[] getAttributes() {
    if (this.attributes == null) {
      return new Field[0];
    }
    return this.attributes;
  }

  /**
   * verify if the config is set correctly
   * @return
   */
  public boolean check() {

    // TODO 支持直接读取 Trajectory Polyline Layer
    if (this.layerType.equals(LayerType.TRAJECTORY_POLYLINE_LAYER)) {
      throw new LayerReaderException("trajectory polyline layer cannot be read directly yet, need to get it from trajectory point layer");
    }

    if (this.sourcePath.trim().length() == 0) {
      throw new LayerReaderException("set source path first");
    }

    if (this.layerType == null) {
      throw new LayerReaderException("set layer type first");
    }

    if (this.layerType.equals(LayerType.TRAJECTORY_POINT_LAYER) && timeField == null) {
      throw new LayerReaderException("time index needs to be set for trajectory point layer");
    }

    // ID字段必须为String类型
    if (!this.idField.getType().equals(String.class.getName())) {
      throw new LayerReaderException("Id field class type must be String");
    }

    return true;
  }

  public Field[] getAllAttributes() {
      return this.getAllAttributes(true);
  }

  /**
   * 获取包含 ID，时间，空间字段的的所有图层字段
   * @return
   */
  public Field[] getAllAttributes(boolean reserveTimeField) {
    List<Field> fs = new ArrayList<>();
    fs.add(idField);
    if (attributes != null && attributes.length > 0) {
      for (Field f: attributes) {
        fs.add(f);
      }
    }
    if (reserveTimeField) fs.add(timeField);
    fs.add(shapeField);
    return fs.toArray(new Field[0]);
  }

}
