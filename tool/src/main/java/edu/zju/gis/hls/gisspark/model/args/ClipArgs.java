package edu.zju.gis.hls.gisspark.model.args;

import lombok.Getter;
import org.kohsuke.args4j.Option;

/**
 * @author Hu
 * @date 2020/7/10
 **/
@Getter
public class ClipArgs extends BaseArgs {

  @Option(name = "-output",usage = "输出文件路径", required = true)
  private String output;

  @Option(name = "-input1",usage = "输入数据,extent", required = true)
  private String input1;

  @Option(name = "-type1",usage = "输入数据类型,extent", required = true)
  private String type1;

  @Option(name = "-input2",usage = "输入数据,target", required = true)
  private String input2;

  @Option(name = "-type2",usage = "输入数据类型,target", required = true)
  private String type2;

  @Option(name = "-crs1",usage = "范围图层数据CRS")
  private String crs1 = "4326";

  @Option(name = "-crs2",usage = "目标图层数据CRS")
  private String crs2 = "4326";

  @Option(name = "-attrReserved",usage = "是否保留范围图层图斑字段", required = true)
  private String areaIndex;

}
