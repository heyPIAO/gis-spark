package edu.zju.gis.hls.trajectory.doc.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-18
 */
@AllArgsConstructor
@Getter
@Setter
public class OsmRecord {
    private Long index;
    private Envelope envelope;

    public OsmRecord(Row row) throws ParseException {
        this.index = row.getAs("osm_id");
        WKTReader reader = new WKTReader();
        String wkt = row.getAs("geom");
        this.envelope = reader.read(wkt.trim()).getEnvelopeInternal();
    }
}
