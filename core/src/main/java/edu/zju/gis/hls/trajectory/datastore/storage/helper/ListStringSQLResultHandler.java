package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import java.util.List;

/**
 * @author Hu
 * @date 2020/9/9
 **/
public class ListStringSQLResultHandler extends SQLResultHandlerImpl<String> {
  @Override
  public boolean handle(List<String> t) {
    this.result = t;
    return true;
  }
}
