package edu.zju.gis.hls.gisspark.model.args;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.kohsuke.args4j.Option;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PgDataLoaderArgs extends DataLoaderArgs{
    @Option(name = "-layerName",usage = "图层名称", required = true)
    private String layerName;

    @Option(name = "-layerAlias",usage = "图层别名", required = true)
    private String layerAlias;

    @Option(name = "-layerTbName",usage = "图层别名", required = true)
    private String tableName;

    @Option(name = "-layerDescription",usage = "图层描述", required = true)
    private String layerDescription;

    @Option(name = "-layerYear",usage = "图层数据年份", required = true)
    private String layerYear;

    @Option(name = "-xzqdm",usage = "行政区代码", required = true)
    private String xzqdm;

    @Option(name = "-xzqmc",usage = "行政区名称", required = true)
    private String xzqmc;

    @Option(name = "-layerTemplate",usage = "图层数据模版", required = true)
    private String layerTemplate;
}
