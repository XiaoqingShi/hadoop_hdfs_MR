package hadoop_mapreduce_wordcount2_Partition_Combiner_CombineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author movw
 * @date 2018年8月16日 下午4:38:23
 * @version 1.0
 */
public class WordCountMain {
	public static void main(String[] args) throws Exception {
		// 获取Job对象
		Job job = Job.getInstance(new Configuration());
		// 设置job的程序入口
		job.setJarByClass(WordCountMain.class);
		// 指明Mapper
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 指明分区
		job.setPartitionerClass(WordCountPartitioner.class);
		job.setNumReduceTasks(2);
		// 指定需要使用combiner，以及用哪个类作为combiner的逻辑
		job.setCombinerClass(WordCountCombiner.class);

		// 如果不设置InputFormat，它默认用的是TextInputFormat.class
		// 大量小文件的切片优化（CombineTextInputFormat）
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

		// 指明Reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 指明输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 启动任务
		job.waitForCompletion(true);
	}
}
