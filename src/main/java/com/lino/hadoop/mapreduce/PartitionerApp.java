package com.lino.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author LINO
 * @Title: MapReduceApp
 * @ProjectName hadoop
 * @Description: TODO
 * @date 2019/3/22
 */
public class PartitionerApp {
    /*
    * map:读取文件
    *
    *
     * 这部分的输入是由mapreduce自动读取进来的
     * 简单的统计单词出现次数<br>
     * KEYIN 默认情况下，是mapreduce所读取到的一行文本的起始偏移量，Long类型，在hadoop中有其自己的序列化类LongWriteable
     * VALUEIN 默认情况下，是mapreduce所读取到的一行文本的内容，hadoop中的序列化类型为Text
     * KEYOUT 是用户自定义逻辑处理完成后输出的KEY，在此处是单词，String
     * VALUEOUT 是用户自定义逻辑输出的value，这里是单词出现的次数，Long
     *
     *
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        public static final LongWritable one = new LongWritable(1);
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            接收到每一行得数据
            String line = value.toString();
//            分割
            String[] words = line.split(" ");

                //将单词输出为key，次数输出为value，这行数据会输到reduce中
                context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));

        }
    }


    public static class myPartitioner extends Partitioner<Text,LongWritable>{

        @Override
        public int getPartition(Text key, LongWritable value, int i) {
            if(key.toString().equals("xiaomi")){
                return 0;
            }
            if(key.toString().equals("huawei")){
                return 1;
            }
            if(key.toString().equals("iphone")){
                return 2;
            }
            return 3;
        }
    }


    /**
    　　* @Description: 归并处理
    　　* @author Lino
    　　* @date 2019/3/22
         * 第一个Text: 是传入的单词名称，是Mapper中传入的
         * 第二个：LongWritable 是该单词出现了多少次，这个是mapreduce计算出来的，比如 hello出现了11次
         * 第三个Text: 是输出单词的名称 ，这里是要输出到文本中的内容
         * 第四个LongWritable： 是输出时显示出现了多少次，这里也是要输出到文本中的内容
    　　*/
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum =0;
            for(LongWritable value:values){
//                求key出现的总和
                sum+=value.get();
            }
//            最终结果输出
            context.write(key,new LongWritable(sum));
        }
    }


    public static void main(String arg[]) throws Exception {
        Configuration conf=new Configuration();

//        清理已经存在的路径
        Path outpath=new Path(arg[1]);
        FileSystem fileSystem=FileSystem.get(conf);
        if(fileSystem.exists(outpath)){
            fileSystem.delete(outpath,true);
            System.out.println("原先输出路径已经删除。。。。。。。");
        }


        Job job = Job.getInstance(conf, "wordconunt");
        job.setJarByClass(PartitionerApp.class);
//        作业处理输入路径
        FileInputFormat.setInputPaths(job,new Path(arg[0]));


//        设置map参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

//        设置reduce参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        通过job设置partitioner
        job.setPartitionerClass(myPartitioner.class);
//        设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);


//        设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path(arg[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
