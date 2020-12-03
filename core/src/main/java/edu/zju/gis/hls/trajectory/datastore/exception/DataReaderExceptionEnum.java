package edu.zju.gis.hls.trajectory.datastore.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Hu
 * @date 2019/6/19
 **/
@Getter
@AllArgsConstructor
public enum  DataReaderExceptionEnum {

  UNKNOWN_READ_EXCEPTION("READ0000","未知数据读取异常","error"),


  SYSTEM_READ_ERROR("READ0001","系统数据读取异常","error"),

  /** 未知异常 */
  FILE_NOT_EXIST("READ0002","文件不存在","error");

  private String errorCode;
  private String errorMsg;
  private String errorType;

}
