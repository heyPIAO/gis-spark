package edu.zju.gis.hls.trajectory.datastore.storage.reader.es;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2020/7/1
 **/
@Getter
@Setter
@ToString
public class ESLayerReaderConfig extends LayerReaderConfig {

    private String masterNode;
    private String port;
    private String[] nodes;
    private String indexName;
    private String typeName;


    @Override
    public boolean check() {
        return super.check() && (masterNode != null && masterNode.trim().length() > 0)
                && (port != null && port.trim().length() > 0)
                && (indexName != null && indexName.trim().length() > 0)
                && (typeName != null && typeName.trim().length() > 0)
                && (nodes != null && nodes.length > 0);
    }

    public ESLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
        super(layerName, sourcePath, layerType);
    }

    @Override
    public String toString() {
        return String.format("http://%s:%s", this.masterNode, this.port);
    }
}
