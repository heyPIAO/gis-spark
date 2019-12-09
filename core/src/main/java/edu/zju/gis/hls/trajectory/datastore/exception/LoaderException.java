package edu.zju.gis.hls.trajectory.datastore.exception;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LoaderException extends RuntimeException {

  private String errorCode;

  public LoaderException(String message) {
    this(LoaderExceptionEnum.SYSTEM_LOAD_ERROR.getErrorCode(), message);
  }

  public LoaderException(LoaderExceptionEnum loaderExceptionEnum) {
    this(loaderExceptionEnum.getErrorCode(), loaderExceptionEnum.getErrorMsg());
  }

  public LoaderException(LoaderExceptionEnum loaderExceptionEnum, String msg) {
    this(loaderExceptionEnum.getErrorCode(), loaderExceptionEnum.getErrorMsg() + ":" + msg);
  }

  public LoaderException(String errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

}