package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import java.util.List;

/**
 * @author Hu
 * @date 2020/7/14
 * JDBC Helper runsql 查询结果的回调接口
 **/
public interface SQLResultHandler<T> {
  boolean handle(List<T> t);
}
