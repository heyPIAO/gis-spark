package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import lombok.Getter;

import java.util.List;

/**
 * @author Hu
 * @date 2020/9/3
 **/
@Getter
public abstract class SQLResultHandlerImpl<S> implements SQLResultHandler<S> {
  protected List<S> result;
}
