package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.hls.udt.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * @author Hu
 * @date 2019/12/17
 * 图层字段
 **/
@Getter
@ToString
public class Field implements Serializable {

    @Setter
    private String name;

    @Setter
    private String alias;

    private String type;

    @Setter
    private int length;

    private int index;

    private boolean exist;

    @Setter
    private FieldType fieldType;

    // 字段默认为普通字段
    public Field(String name) {
        this(name, FieldType.NORMAL_FIELD);
    }

    public Field(Field f) {
        this(f.name, f.alias, f.type, f.length, f.index, f.fieldType);
    }

    public Field(String name, String type) {
        this(name, name, type, Term.FIELD_LENGTH, Term.FIELD_EXIST, FieldType.NORMAL_FIELD);
    }

    public Field(String name, String type, FieldType fieldType) {
        this(name, name, type, Term.FIELD_LENGTH, Term.FIELD_EXIST, fieldType);
    }

    // 字段默认存在
    public Field(String name, FieldType fieldType) {
        this(name, Term.FIELD_EXIST, fieldType);
    }

    // 字段默认alias与name同名
    public Field(String name, Integer index, FieldType fieldType) {
        this(name, name, index, fieldType);
    }

    // 字段默认为String类且默认长度为255
    public Field(String name, String alias, Integer index, FieldType fieldType) {
        this(name, alias, String.class.getName(), Term.FIELD_LENGTH, index, fieldType);
    }

    public Field(String name, String alias, String type, int length, int index, FieldType fieldType) {
        this.name = name;
        this.alias = alias;
        this.type = type;
        this.length = length;
        this.index = index;
        this.fieldType = fieldType;
        this.exist = (index >= Term.FIELD_LAST);
    }

    public boolean isExist() {
        return this.exist;
    }

    public void setIndex(int index) {
        this.index = index;
        this.exist = (index >= Term.FIELD_LAST);
    }

    public boolean isTemporal() {
        return this.fieldType.equals(FieldType.TEMPORAL_FIELD);
    }

    public void setType(Class c) {
        this.setType(c.getName());
    }

    public void setType(String className) {
        this.type = className;
    }

    // TODO Field 同名就认为是同一个字段
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Field) {
            Field of = (Field) obj;
            return of.getName().equals(this.name);
        }
        return false;
    }

    /**
     * 比较SparkSQL数据类型与用户定义的数据类型是否一致
     *
     * @param sourceClass
     * @param uClass
     * @return reference: #{org.apache.spark.sql.Row}
     */
    public static boolean equalFieldClass(DataType sourceClass, String uClass) {
        if (sourceClass.sameType(DataTypes.StringType)) {
            return uClass.equals(String.class.getName())
              || uClass.equals(Geometry.class.getName())
              || uClass.equals(org.locationtech.jts.geom.Point.class.getName())
              || uClass.equals(org.locationtech.jts.geom.MultiPoint.class.getName())
              || uClass.equals(org.locationtech.jts.geom.LineString.class.getName())
              || uClass.equals(org.locationtech.jts.geom.MultiLineString.class.getName())
              || uClass.equals(org.locationtech.jts.geom.Polygon.class.getName())
              || uClass.equals(org.locationtech.jts.geom.MultiPolygon.class.getName());
        } else if (sourceClass.sameType(DataTypes.IntegerType)) {
            return uClass.equals(Integer.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.LongType)) {
            return uClass.equals(Long.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.DoubleType)) {
            return uClass.equals(Double.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.FloatType)) {
            return uClass.equals(Float.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.BooleanType)) {
            return uClass.equals(Boolean.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.DateType)) {
            return uClass.equals(Date.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass.sameType(DataTypes.TimestampType)) {
            return uClass.equals(Timestamp.class.getName()) || uClass.equals(String.class.getName());
        } else if (sourceClass instanceof DecimalType) {
            return uClass.equals(Double.class.getName()) || uClass.equals(String.class.getName());
        } else {
            throw new GISSparkException("Unsupport DataType: " + sourceClass.typeName());
        }
    }

    public static DataType converFieldTypeToDataType(Field field) {
        return converFieldTypeToDataType(field, false);
    }

    /**
     * 将用户定义的图层字段类型转为SparkSQL的DataType
     * TODO 映射关系写到 enum 里面去
     * @param field
     * @return reference: #{org.apache.spark.sql.Row}
     */
    public static DataType
    converFieldTypeToDataType(Field field, boolean geomReserved) {

        String fieldType = field.getType();

        if (field.getFieldType().equals(FieldType.SHAPE_FIELD)) {
            if (!geomReserved) return DataTypes.StringType;
//            if (fieldType.equals(org.locationtech.jts.geom.Geometry.class.getName())) return DataTypes.BinaryType;
            if (fieldType.equals(org.locationtech.jts.geom.Geometry.class.getName())) return new GeometryUDT();
            if (fieldType.equals(org.locationtech.jts.geom.Point.class.getName())) return new PointUDT();
            if (fieldType.equals(org.locationtech.jts.geom.LineString.class.getName())) return new PolylineUDT();
            if (fieldType.equals(org.locationtech.jts.geom.Polygon.class.getName())) return new PolygonUDT();
            if (fieldType.equals(org.locationtech.jts.geom.MultiPoint.class.getName())) return new MultiPointUDT();
            if (fieldType.equals(org.locationtech.jts.geom.MultiLineString.class.getName())) return new MultiPolylineUDT();
            if (fieldType.equals(org.locationtech.jts.geom.MultiPolygon.class.getName())) return new MultiPolygonUDT();
            if (fieldType.equals(TemporalPoint.class.getName())) return new TemporalPointUDT();
            if (fieldType.equals(TemporalLineString.class.getName())) return new TemporalPolylineUDT();
            throw new GISSparkException("Unsupport Geometry DataType: " + fieldType);
        }

        if (fieldType.equals(String.class.getName())) {
            return DataTypes.StringType;
        }  else if (fieldType.equals(Integer.class.getName())) {
            return DataTypes.IntegerType;
        } else if (fieldType.equals(Long.class.getName())) {
            return DataTypes.LongType;
        } else if (fieldType.equals(Double.class.getName())) {
            return DataTypes.DoubleType;
        } else if (fieldType.equals(Float.class.getName())) {
            return DataTypes.FloatType;
        } else if (fieldType.equals(Boolean.class.getName())) {
            return DataTypes.BooleanType;
        } else if (fieldType.equals(Date.class.getName())) {
            return DataTypes.DateType;
        } else if (fieldType.equals(java.sql.Date.class.getName())) {
            return DataTypes.DateType;
        } else if (fieldType.equals(Timestamp.class.getName())) {
            return DataTypes.TimestampType;
        } else {
            throw new GISSparkException("Unsupport Field Type: " + fieldType);
        }
    }

    public boolean isNumeric() {
        return this.type.equals(Integer.class.getName())
                || this.type.equals(Double.class.getName())
                || this.type.equals(Float.class.getName());
    }

    public boolean isGeometry() {
        return this.type.equals(Geometry.class.getName());
    }

}
