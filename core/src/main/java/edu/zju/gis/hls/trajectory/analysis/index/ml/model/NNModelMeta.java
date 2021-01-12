package edu.zju.gis.hls.trajectory.analysis.index.ml.model;

import edu.zju.gis.hls.trajectory.analysis.util.FileUtil;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.IOException;

/**
 * @author Hu
 * @date 2021/1/9
 **/
@Getter
@Setter
public class NNModelMeta {

  private String id;
  private int bias[][];

  public NNModelMeta() {
  }

  public NNModelMeta(String line) {
    String[] fs = line.split(",");
    this.id = fs[0];
    if (fs.length == 3) {
      this.bias = new int[1][2];
      this.bias[0][0] = Integer.valueOf(fs[1]);
      this.bias[0][1] = Integer.valueOf(fs[2]);
    } else if (fs.length == 5) {
      this.bias = new int[2][2];
      this.bias[0][0] = Integer.valueOf(fs[1]);
      this.bias[0][1] = Integer.valueOf(fs[2]);
      this.bias[1][0] = Integer.valueOf(fs[3]);
      this.bias[1][1] = Integer.valueOf(fs[4]);
    } else {
      this.bias = new int[3][2];
      this.bias[0][0] = Integer.valueOf(fs[1]);
      this.bias[0][1] = Integer.valueOf(fs[2]);
      this.bias[1][0] = Integer.valueOf(fs[3]);
      this.bias[1][1] = Integer.valueOf(fs[4]);
      this.bias[2][0] = Integer.valueOf(fs[5]);
      this.bias[2][1] = Integer.valueOf(fs[6]);
    }
  }

  @Override
  public String toString() {
    int shapeDim = bias.length;
    if (shapeDim == 1) {
      return String.format("%s,%d,%d", id, bias[0][0], bias[0][1]);
    } else if (shapeDim == 2) {
      return String.format("%s,%d,%d,%d,%d", id, bias[0][0], bias[0][1], bias[1][0], bias[1][1]);
    } else if (shapeDim == 3) {
      return String.format("%s,%d,%d,%d,%d,%d,%d", id, bias[0][0], bias[0][1], bias[1][0], bias[1][1], bias[2][0], bias[2][1]);
    } else {
      throw new GISSparkException("Unsupported bias with dimension: " + shapeDim);
    }
  }

  public void save(String dir) throws IOException {
    FileUtil.write(dir + File.separator + this.id + "_meta", this.toString());
  }
}
