package edu.zju.gis.hls.trajectory.datastore.storage.writer.shp;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShpWriterConfig extends LayerWriterConfig {
    @Getter
    @Setter
    private String encodeType = "UTF8";
    public ShpWriterConfig(String sinkPath){
        super(sinkPath);
    }
}
