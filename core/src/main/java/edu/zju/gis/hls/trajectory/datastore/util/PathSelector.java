package edu.zju.gis.hls.trajectory.datastore.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class PathSelector {
    public static List<String> stripPath(String pathes, String prefix, String suffix) {
        String path = null;
        try {
            path = java.net.URLDecoder.decode(pathes, "GBK");
//            path = pathes;
        } catch (UnsupportedEncodingException e) {
            log.error("decode error:" + e.getMessage());
        }

        List<String> paths;
        if (path.contains(";")) {
            paths = Arrays.asList(path.split(";"));
        } else {
            paths = new ArrayList<>();
            paths.add(path);
        }

        //判断路径是否为文件夹，支持二级目录下所有文件路径的获取
        List<String> paths2File = new ArrayList<>();
        for (String subPath : paths) {
            File subFile = new File(subPath.replace(prefix, ""));
            if (subFile.exists()) {
                if (subFile.isFile() && subFile.getAbsolutePath().endsWith(suffix)) {
                    paths2File.add(subFile.getAbsolutePath());
                } else {
                    File[] subSubFiles = subFile.listFiles();
                    for (File subSubFile : subSubFiles) {
                        if (subSubFile.isFile() && subFile.getAbsolutePath().endsWith(suffix))
                            paths2File.add(subSubFile.getAbsolutePath());
                    }
                }
            }
        }
        return paths2File;
    }
}
