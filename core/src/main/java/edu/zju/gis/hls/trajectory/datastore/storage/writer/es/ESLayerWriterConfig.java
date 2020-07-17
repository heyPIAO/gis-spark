package edu.zju.gis.hls.trajectory.datastore.storage.writer.es;

import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/7/1
 **/
@Getter
@Setter
public class ESLayerWriterConfig extends LayerWriterConfig {
    private String masterNode;
    private String port;
    private String indexName;
    private String typeName;
    private String resource;

    private Integer NUMBER_OF_SHARDS = 1;
    private Integer NUMBER_OF_REPLICA = 0;
    private Integer INDEX_REFRESH_INTERVAL = -1;

    public ESLayerWriterConfig(String masterNode, String port, String indexName, String typeName) {
        this.masterNode = masterNode;
        this.port = port;
        this.indexName = indexName;
        this.typeName = typeName;
        this.resource = String.format("%s/%s", indexName, typeName);
        this.sinkPath = String.format(SourceType.ES + "http://%s:%s/%s/%s", masterNode, port, indexName, typeName);
    }
}
