package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import java.util.List;

/**
 * @author Hu
 * @date 2020/9/3
 **/
public class VoidSQLResultHandlerImpl extends SQLResultHandlerImpl<String> {
  @Override
  public boolean handle(List<String> t) {
    return true;
  }
}
