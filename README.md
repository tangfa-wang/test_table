# test_table
这是一个数仓用户点击购买表
/**
 * WordCount.java
 * com.hainiu.day01
 * Copyright (c) tangfa版权所有.
*/

package com.hainiu.day01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 写mapreduce程序实现单词统计
 * @author    潘牛
 * @Date	 2020年12月28日 	 
 */
public class WordCount {
	/*
	 * KEYIN, VALUEIN
	 * one world --->   0   one world
	 * one dream --->   11  one dream
	 * 
	 * KEYIN --> 行字节偏移量 ---》 long  --》LongWrtiable (里面封装了long类型)
	 * VALUEIN --> 一行的数据 ---> 字符串   --》Text (里面封装了String类型)
	 * 
	 * LongWrtiable、Text 是hadoop自带的序列化类型，实现了Hadoop的序列化和反序列化
	 *
	 * 
	 * KEYOUT, VALUEOUT
	 * 取决于业务需求
	 * 因为本次是统计单词的个数
	 * 
	 * KEYOUT：单词  ---> 字符串   --》Text (里面封装了String类型)
	 * VALUEOUT：数值    --》 long  --》LongWrtiable (里面封装了long类型)
	 * 
	 * 
	 * 读取Text文件的是TextInputFormat
	 * public class TextInputFormat extends FileInputFormat<LongWritable, Text>
	 * public abstract class FileInputFormat<K, V> extends InputFormat<K, V>
	 * 
	 * KEYIN: LongWritable (FileInputFormat key的泛型)
	 * VALUEIN： Text (FileInputFormat value的泛型)
	 * 
	 * public class TextInputFormat{
	 *   public RecordReader<LongWritable, Text> 
		    createRecordReader(InputSplit split,
		                       TaskAttemptContext context) {
			。。。。
			// 返回读取Text文件的具体RecordReader对象
		    return new LineRecordReader(recordDelimiterBytes);
  		}
  		
  		 // 在split分片的时候，会判断Text文件的压缩格式是否支持split，如果不支持，那一个文件一个分片（一个Mapper 对象）
		  protected boolean isSplitable(JobContext context, Path file) {
		    final CompressionCodec codec =
		      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		    if (null == codec) {
		      return true;
		    }
		    return codec instanceof SplittableCompressionCodec;
		  }
	 * 
	 * 
	 * }
	 * 
	 * 
	 * 
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		/**
		 * mapper输出的key: 单词
		 */
		Text keyOut = new Text();
		
		/**
		 * mapper输出的value：数值（1）
		 */
		LongWritable valueOut = new LongWritable(1L);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("mapper input==>" + key.get() + ", " + value.toString());
			// one world
			String line = value.toString();
			// {one, world}
			String[] arr = line.split(" ");
			
			for(String word : arr){
				
				keyOut.set(word);
				// 通过context.write（）输出数据
				context.write(keyOut, valueOut);
				
				System.out.println("mapper output==> " + word + ", " + valueOut.get());
			}
			
			
		}
		
	}
	
	/*
	 * KEYIN, VALUEIN
	 * reduce的 KEYIN, VALUEIN 与 mapper 输出的 KEYOUT, VALUEOUT一致
	 * KEYIN：单词 --》 Text
	 * VALUEIN:数值 ——》 LongWritable
	 * 
	 * KEYOUT, VALUEOUT
	 * reduce的keyout valueout 取决于业务需求
	 * 最终是统计单词的个数
	 * 
	 * KEYOUT：单词  --》 Text
	 * 
	 * VALUEOUT： 汇总的数值 -->LongWritable
	 * 
	 */
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
		LongWritable valueOut = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
//			key: mapper 输出的key  --》 one
//			values: 按照mapper 输出的key，把value 汇总在一起  [1,1,1,1]
			
			long sum = 0L;
			
			StringBuilder sb = new StringBuilder();
			sb.append("reducer input==>")
				.append(key.toString())
				.append(", [");
			for(LongWritable w : values){
				long num = w.get();
				sb.append(num).append(",");
				sum += num;
			}
			
			sb.deleteCharAt(sb.length()-1);
			sb.append("]");
			
			System.out.println(sb.toString());
			
			valueOut.set(sum);
			
			context.write(key, valueOut);
			System.out.println("mapper output==> " + key + ", " + valueOut.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// 创建Configuration对象
		// 用于加载公共配置
//	    addDefaultResource("core-default.xml");
//	    addDefaultResource("core-site.xml");
		Configuration conf = new Configuration();
		
		// 创建运行mapreduce任务的Job对象
		// 加载运行mapreduce任务所需的配置
//	    Configuration.addDefaultResource("mapred-default.xml");
//	    Configuration.addDefaultResource("mapred-site.xml");
//	    Configuration.addDefaultResource("yarn-default.xml");
//	    Configuration.addDefaultResource("yarn-site.xml");
		Job job = Job.getInstance(conf, "wordcount");
		
		// 设置任务运行的class
		job.setJarByClass(WordCount.class);
		
		// 设置任务运行的mapperclass
		job.setMapperClass(WordCountMapper.class);
		
		// 设置任务运行的reducerclass
		job.setReducerClass(WordCountReducer.class);
		
		// 设置reduce的个数，不写默认是1
		job.setNumReduceTasks(2);
		
		// 设置mapperoutputkeyclass
		job.setMapOutputKeyClass(Text.class);
		
		// 设置 mapperoutputvalueclass
		job.setMapOutputValueClass(LongWritable.class);
		
		// 设置最终输出的keyclass
		job.setOutputKeyClass(Text.class);
		
		// 设置最终输出的valueclass
		job.setOutputValueClass(LongWritable.class);
		
		// 设置输入的InputFormatclass，如果不写默认就是TextInputFormat
		// TextInputFormat 是代表读取的文件是文本格式
		job.setInputFormatClass(TextInputFormat.class);
		
		// 设置输出的OutputFormatclass，如果不写默认就是TextOutputFormat
		// TextOutputFormat是代表写入的文件是文本格式
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 设置任务的输入目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		
		Path outputPath = new Path(args[1]);
		
		// 自动删除输出目录
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			// 递归删除输出目录
			fs.delete(outputPath, true);
			System.out.println("delete outputpath ==> " + outputPath.toString() + " success!");
		}
		
		
		// 设置任务的输出目录
		// 输出目录不能存在，存在就报错
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// 提交mapreduce任务
		boolean isSuccess = job.waitForCompletion(false);
		
		System.exit(isSuccess ? 0 : 1);
		
		
		
	}

}
