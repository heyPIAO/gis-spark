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

  @Option(name = "-output",usage = "输出文件参数定义,json", required = true)
  private String output;

  @Option(name = "-input1",usage = "范围图层输入数据参数定义,json", required = true)
  private String input1;

  @Option(name = "-input2",usage = "被擦除图层输入数据参数定义,json", required = true)
  private String input2;

  @Option(name = "-attrReserved",usage = "是否保留范围图层图斑字段")
  private Boolean attrReserved = Boolean.FALSE;

  @Option(name = "-indexBaseLayer",usage = "是否为被裁切图层构建索引")
  private Boolean indexBaseLayer = Boolean.TRUE;

}
