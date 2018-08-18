package hadoop_mapreduce_wordcount2_Partition_Combiner_CombineTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author movw
 * @date 2018年8月16日 下午6:06:11
 * @version 1.0
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable v : values) {
			count += v.get();
		}

		context.write(key, new IntWritable(count));
	}

}
