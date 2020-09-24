package edu.zju.gis.hls.trajectory.model;

/**
 * 标记Geometry 类型
 * Created by ylj on 2017/6/23.
 */
public enum GeometryType {

    POINT(1),
    LINESTRING(2),
    POLYGON(3),
    MULTIPOINT(4),
    MULTILINESTRING(5),
    MULTIPOLYGON(6),
    MULTIGEOMETRY(7);

    private int typeCode;
    GeometryType(int code){
        this.typeCode=code;
    }

    public int getTypeCode(){
        return this.typeCode;
    }

}