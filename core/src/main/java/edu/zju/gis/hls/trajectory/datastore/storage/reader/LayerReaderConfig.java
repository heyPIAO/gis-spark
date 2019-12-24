package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import lombok.Getter;
import lombok.Setter;
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
public abstract class LayerReaderConfig implements Serializable {

  private String layerId = UUID.randomUUID().toString();
  private String layerName;
  protected String sourcePath;
  protected LayerType layerType;
  protected Field shapeField = Term.FIELD_DEFAULT_SHAPE;
  protected Field idField = Term.FIELD_DEFAULT_ID;
  protected Field startTimeField = Term.FIELD_DEFAULT_START_TIME;
  protected Field endTimeField = Term.FIELD_DEFAULT_END_TIME;
  protected Field timeField = Term.FIELD_DEFAULT_TIME;
  protected CoordinateReferenceSystem crs = Term.DEFAULT_CRS;
  protected Field[] attributes; // 不包括 shape, id, startTime, endTime, Time 的所有需要读取的 Field 信息

  public LayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    this.sourcePath = sourcePath;
    this.layerType = layerType;
    this.layerName = layerName;
  }

  /**
   * verify if the config is set correctly
   * @return
   */
  public boolean check() {

    if (this.sourcePath.trim().length() == 0) {
      throw new LayerReaderException("set source path first");
    }

    if (this.layerType == null) {
      throw new LayerReaderException("set layer type first");
    }

    if (this.layerType.equals(LayerType.TRAJECTORY_POINT_LAYER) && timeField == null) {
      throw new LayerReaderException("time index needs to be set for trajectory point layer");
    }

    if (this.layerType.equals(LayerType.TRAJECTORY_POLYLINR_LAYER) && (startTimeField == null || endTimeField == null)) {
      throw new LayerReaderException("start time index or end time index needs to be set for trajectory polyline layer");
    }

    return true;
  }

  public Field[] getAllAttributes() {
    List<Field> fs = new ArrayList<>();
    for (Field f: attributes) {
      fs.add(f);
    }
    fs.add(idField);
    fs.add(shapeField);
    fs.add(startTimeField);
    fs.add(endTimeField);
    fs.add(timeField);
    return fs.toArray(new Field[0]);
  }

}
