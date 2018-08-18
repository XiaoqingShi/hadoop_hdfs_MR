package hadoop_mapreduce_saltotal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author movw
 * @date 2018年8月16日 下午6:25:13
 * @version 1.0
 */
public class SalaryTotalMain {
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		// 开启map端输出压缩
		configuration.setBoolean("mapreduce.map.output.compress", true);
		// 设置map端输出压缩方式
		configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

		Job job = Job.getInstance(configuration);

		// 设置程序入口
		job.setJarByClass(SalaryTotalMain.class);

		// 设置map
		job.setMapperClass(SalaryTotalMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定job的reducer和输出的类型 k4 v4
		job.setReducerClass(SalaryTotalReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// 指定job的输入和输出的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 执行任务
		job.waitForCompletion(true);
	}

}
