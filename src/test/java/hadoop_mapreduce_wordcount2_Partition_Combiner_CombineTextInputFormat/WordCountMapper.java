package hadoop_mapreduce_wordcount2_Partition_Combiner_CombineTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author movw
 * @date 2018年8月16日 下午4:37:50
 * @version 1.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 获取一行的数据
		String v3 = value.toString();
		// 分离单词
		String[] words = v3.split(" ");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
