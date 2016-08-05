package edu.berkeley.ground.ingest;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test {

  public static void main(String[] args) throws IOException {
    
    String file = "hello/Nasaavrofile";
    
    Path path = new Path(file);
    FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020"), new Configuration());
    FileStatus[] statuses = fs.listStatus(path);
    
    System.out.println(statuses[0].getPath());
    System.out.println(statuses[0].getAccessTime());
    System.out.println(statuses[0].getLen());
    System.out.println(statuses[0].getModificationTime());
    System.out.println(statuses[0].getOwner());
    

  }

}
