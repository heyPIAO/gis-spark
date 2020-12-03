package edu.zju.gis.hls.trajectory.datastore.exception;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2019/6/19
 **/
@Getter
@Setter
public class DataReaderException extends RuntimeException {

  private String errorCode;

  public DataReaderException(String message) {
    this(DataReaderExceptionEnum.SYSTEM_READ_ERROR.getErrorCode(), message);
  }

  public DataReaderException(DataReaderExceptionEnum readerExceptionEnum){
    this(readerExceptionEnum.getErrorCode(), readerExceptionEnum.getErrorMsg());
  }

  public DataReaderException(DataReaderExceptionEnum readerExceptionEnum, String msg){
    this(readerExceptionEnum.getErrorCode(), readerExceptionEnum.getErrorMsg() + ":" + msg);
  }

  public DataReaderException(String errorCode, String message){
    super(message);
    this.errorCode = errorCode;
  }


}
