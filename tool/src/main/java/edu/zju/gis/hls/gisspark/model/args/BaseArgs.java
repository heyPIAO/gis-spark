package edu.zju.gis.hls.gisspark.tool.args;

import com.sun.deploy.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * @author Hu
 * @date 2020/7/9
 **/
@Slf4j
public abstract class BaseArgs implements Serializable {

  public static <T extends BaseArgs> T initArgs(String[] args, Class<T> clz) {
    try {
      Constructor con = clz.getConstructor();
      T mArgs = (T) con.newInstance();
      CmdLineParser parser = new CmdLineParser(mArgs);
      parser.parseArgument(args);
      return mArgs;
    } catch (NoSuchMethodException | CmdLineException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      log.error(e.getLocalizedMessage());
      throw new ToolFailedException("init arguments failed: " + StringUtils.join(Arrays.asList(args), ";"));
    }
  }

}
