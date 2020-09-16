package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.awt.image.BufferedImage;
import java.io.Serializable;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-16
 */
@Getter
@Setter
@AllArgsConstructor
public class RasterImage implements Serializable {
    transient private BufferedImage img;
}
