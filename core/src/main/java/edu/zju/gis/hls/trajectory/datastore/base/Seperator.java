package edu.zju.gis.hls.trajectory.datastore.base;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2019/11/12
 **/
public enum Seperator {

  SINGLE_LINE("\n"), TAB("\t"), COMPLEX_LINE("\r\n");

  Seperator(String value) {
    this.value = value;
  }

  @Getter
  @Setter
  private String value;

  @Override
  public String toString() {
    return this.value;
  }

}
