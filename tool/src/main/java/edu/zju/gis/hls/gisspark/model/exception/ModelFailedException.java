package edu.zju.gis.hls.gisspark.tool.exception;

import java.util.Arrays;

/**
 * @author Hu
 * @date 2019/9/4
 * 并行模型失败的Exception
 **/
public class ModelFailedException extends RuntimeException {

  public ModelFailedException(String message) {
    super("Job failed: " + message);
  }

  public ModelFailedException(Class clz, String method, String msg, String... data){
    this(String.format("%s, %s, %s，%s", clz.getName(), method, Arrays.toString(data), msg));
  }

}
