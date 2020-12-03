package edu.zju.gis.hls.trajectory.datastore.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Hu
 * @date 2019/6/19
 **/
@Getter
@AllArgsConstructor
public enum LoaderExceptionEnum {

    /**
     * 未知异常
     */
    UNKNOWN_LOAD_EXCEPTION("LOAD0000", "未知数据装载异常", "error"),

    /**
     * 未知异常
     */
    SYSTEM_LOAD_ERROR("LOAD0001", "系统数据装载异常", "error"),

    /**
     * 不支持的字段类型
     */
    UNSUPPORTED_FIELD_TYPE("LOAD0002", "不支持的字段类型", "error"),

    /**
     * 不支持的字段类型
     */
    UNVALID_FIELD_VALUE("LOAD0003", "不支持的字段值", "error"),

    /**
     * 不支持的空间对象类型
     */
    UNSUPPORTED_FEATURE_TYPE("LOAD0004", "不支持的空间对象类型", "error");

    private String errorCode;
    private String errorMsg;
    private String errorType;

}
