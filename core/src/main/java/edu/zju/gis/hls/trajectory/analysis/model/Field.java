package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
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

  @Setter
  private String type;

  @Setter
  private int length;

  private int index;

  private boolean exist;

  @Setter
  private FieldType fieldType;

  // 字段默认为普通字段
  public Field(String name) {
    this(name, FieldType.NORMA_FIELD);
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

  public void setType(Class c) {
    this.setType(c.getName());
  }

  public void setType(String className) {
    this.type = className;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Field) {
      Field of = (Field) obj;
      return of.getIndex() == this.index
        && of.getName().equals(this.name)
        && of.getAlias().equals(this.getAlias())
        && of.getType().equals(this.type)
        && of.getLength() == this.length;
    }
    return false;
  }

  /**
   * 比较SparkSQL数据类型与用户定义的数据类型是否一致
   * @param sourceClass
   * @param uClass
   * @return
   */
  public static boolean equalFieldClass(DataType sourceClass, String uClass) {
    if (sourceClass.sameType(DataTypes.StringType)) {
      return uClass.equals(String.class.getName()) || uClass.equals(Geometry.class.getName());
    } else if (sourceClass.sameType(DataTypes.IntegerType)) {
      return uClass.equals(Integer.class.getName());
    } else if (sourceClass.sameType(DataTypes.LongType)) {
      return uClass.equals(Long.class.getName());
    } else if (sourceClass.sameType(DataTypes.DoubleType)){
      return uClass.equals(Double.class.getName());
    } else if (sourceClass.sameType(DataTypes.FloatType)){
      return uClass.equals(Float.class.getName());
    } else if (sourceClass.sameType(DataTypes.BooleanType)) {
      return uClass.equals(Boolean.class.getName());
    } else if (sourceClass.sameType(DataTypes.TimestampType)) {
      return uClass.equals(Date.class.getName()) || uClass.equals(String.class.getName());
    } else {
      throw new GISSparkException("Unsupport DataType: " + sourceClass.typeName());
    }
  }
}
