package edu.zju.gis.hls.trajectory.doc.skr;

import edu.zju.gis.hls.trajectory.doc.skr.entity.CheckPoint;
import edu.zju.gis.hls.trajectory.doc.skr.entity.Region;
import lombok.Getter;
import org.locationtech.jts.io.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-15
 */
@Getter
public class TestData {
    private List<Region> regions;
    private List<CheckPoint> points;

    public TestData() throws IOException, ParseException {
        BufferedReader br1 = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/regions.tsv")));
        this.regions = new ArrayList<>();
        String line;
        while ((line = br1.readLine()) != null) {
            Region region = new Region(line);
            this.regions.add(region);
        }
        br1.close();
        BufferedReader br2 = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/points.tsv")));
        this.points = new ArrayList<>();
        while ((line = br2.readLine()) != null) {
            CheckPoint point = new CheckPoint(line);
            this.points.add(point);
        }
        br2.close();
    }
}
