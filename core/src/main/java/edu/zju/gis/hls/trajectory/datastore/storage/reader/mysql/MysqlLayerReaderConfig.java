package edu.zju.gis.hls.trajectory.datastore.storage.reader.mysql;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Zhou
 * @date 2020/7/19
 **/

@Getter
@Setter
@ToString
@NoArgsConstructor
public class MysqlLayerReaderConfig extends LayerReaderConfig {

    private String dbtable;
    private String username;
    private String password;

    @Override
    public boolean check() {
        return super.check() && (dbtable !=null && dbtable.trim().length() > 0)
                && (username != null && username.trim().length() > 0)
                && (password != null && password.trim().length() > 0);
    }

    public MysqlLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
        super(layerName, sourcePath, layerType);
    }

}
