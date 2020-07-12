package edu.zju.gis.hls.gisspark.model.args;

import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Option;

/**
 * @author Hu
 * @date 2020/7/10
 **/
@Getter
@ToString
public class EraseArgs extends BaseArgs {

  @Option(name = "-output",usage = "输出文件路径", required = true)
  private String output;

  @Option(name = "-input1",usage = "范围图层输入数据参数定义,json", required = true)
  private String input1;

  @Option(name = "-input2",usage = "被裁剪图层输入数据参数定义,json", required = true)
  private String input2;

  @Option(name = "-attrReserved",usage = "是否保留范围图层图斑字段", required = true)
  private boolean attrReserved;

}
