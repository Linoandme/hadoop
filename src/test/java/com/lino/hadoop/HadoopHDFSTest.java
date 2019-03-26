package com.lino.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * @author LINO
 * @Title: HadoopHDFSTest
 * @ProjectName hadoop
 * @Description: TODO
 * @date 2019/3/21
 */
public class HadoopHDFSTest {

    public static final String  HDFS_PATH="hdfs://192.168.222.130:8020";
    FileSystem fileSystem=null;
    Configuration configuration=null;
    @Test
    public void mkdirs()throws Exception{

        fileSystem.mkdirs(new Path("/hdava/aa"));
      /*  boolean exists = fileSystem.exists(new Path("/ha/l"));
        System.out.println(exists);*/

    }

    @Test
    public void createFi()throws Exception{
        FSDataOutputStream outp = fileSystem.create(new Path("/hdava/aa/aa.text"));
        System.out.println("create.....");
        outp.write("hello java".getBytes());
        outp.flush();
        outp.close();
    }
    @Before
    public void startup() throws Exception{
        System.out.println("start.....");
        configuration=new Configuration();
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configuration,"root");
        System.out.println("start.....");
    }
    @After
    public void downtodo() throws Exception{
         fileSystem=null;
         configuration=null;
         System.out.println("down  over.....");
    }
}
