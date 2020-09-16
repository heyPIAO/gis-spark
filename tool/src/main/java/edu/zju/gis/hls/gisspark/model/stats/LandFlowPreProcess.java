package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author Hu
 * @date 2020/9/15
 * 二调数据关联生成merge表
 **/
@Slf4j
public class LandFlowPreProcess extends BaseModel<LandFlowPreProcessArgs> {

  public LandFlowPreProcess(SparkSessionType type, String[] args) {
    super(type, args);
  }

  @Override
  protected void run() throws Exception {

    LayerReaderConfig lxdwConfig = LayerFactory.getReaderConfig(this.arg.getLxdwReaderConfig());
    LayerReaderConfig xzdwConfig = LayerFactory.getReaderConfig(this.arg.getXzdwReaderConfig());
    LayerReaderConfig dltbConfig = LayerFactory.getReaderConfig(this.arg.getDltbReaderConfig());

    LayerReader lxdwReader = LayerFactory.getReader(this.ss, lxdwConfig);
    LayerReader xzdwReader = LayerFactory.getReader(this.ss, xzdwConfig);
    LayerReader dltbReader = LayerFactory.getReader(this.ss, dltbConfig);

    Layer lxdwLayer = lxdwReader.read();
    Layer xzdwLayer = xzdwReader.read();
    Layer dltbLayer = dltbReader.read();

    Dataset<Row> lxdwRf = lxdwLayer.toDataset(this.ss);
    lxdwRf.registerTempTable("lxdw");
    Dataset<Row> xzdwRf = xzdwLayer.toDataset(this.ss);
    xzdwRf.registerTempTable("xzdw");
    Dataset<Row> dltbRf = dltbLayer.toDataset(this.ss);
    dltbRf.registerTempTable("dltb");

//    Dataset<Row> re = this.ss.sql("select * from dltb where BSM = \"115679835\"");
//    List<Row> _r = re.collectAsList();
    // TODO 根据字段关联
    this.ss.sql("CREATE TABLE if not exists merges as \n" +
      "(select d.bsm,d.zldwdm,d.tbbh, d.dlbm as tb_dlbm,d.tbmj as tb_dlmj,l.bsm as lx_id,l.dlbm as lx_dlbm,l.mj as lx_mj,\n" +
      "x.bsm as xz_id,x.dlbm as xz_dlbm,x.cd as xz_cd,x.kd as xz_kd, d.wkt as tb_wkt, l.wkt as lx_wkt,\n" +
      "x.wkt as xz_wkt,d.tkxs as tb_tkcs,x.kcbl as xz_kcbl\n" +
      "from dltb as d \n" +
      "LEFT JOIN lxdw as l on d.zldwdm=l.zldwdm and d.tbbh=l.zltbbh\n" +
      "LEFT JOIN xzdw as x on \n" +
      "(d.zldwdm=x.kctbdwdm1 and d.tbbh=x.kctbbh1) or (d.zldwdm=x.kctbdwdm2 and d.tbbh=x.kctbbh2))");

    Dataset<Row> mergeResult = this.ss.table("merges");
    List<Row> r = mergeResult.collectAsList();
    log.info(String.valueOf(r.size()));
//    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getWriterConfig());
//    LayerWriter writer = LayerFactory.getWriter(this.ss, writerConfig);
//    writer.write(dltbLayer);
  }


  public static void main(String[] args) throws Exception {
    LandFlowPreProcess preProcess = new LandFlowPreProcess(SparkSessionType.LOCAL, args);
    preProcess.exec();
  }

}
