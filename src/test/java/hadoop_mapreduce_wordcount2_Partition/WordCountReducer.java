package hadoop_mapreduce_wordcount2_Partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author movw
 * @date 2018年8月16日 下午4:38:14
 * @version 1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable v : values) {
			total += v.get();
		}
		context.write(new Text(key), new IntWritable(total));
	}

}
