package edu.berkeley.ground.common.util;

import edu.berkeley.ground.common.exception.GroundException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import play.libs.Json;

public class ModelTestUtils {

  public static String readFromFile(String filename) throws GroundException {
    try {
      String content = new Scanner(new File(filename)).useDelimiter("\\Z").next();

      return content;
    } catch (FileNotFoundException e) {
      throw new GroundException("File " + filename + " not found");
    }
  }

  public static Object convertFromStringToClass(String body, Class<?> klass) {
    return Json.fromJson(Json.parse(body), klass);
  }

  public static String convertFromClassToString(Object object) {
    return Json.stringify(Json.toJson(object));
  }
}
