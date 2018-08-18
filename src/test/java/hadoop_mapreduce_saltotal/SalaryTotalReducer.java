package hadoop_mapreduce_saltotal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author movw
 * @date 2018年8月16日 下午6:25:32
 * @version 1.0
 */
public class SalaryTotalReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable k3, Iterable<IntWritable> v3, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable v : v3) {
			total += v.get();
		}
		// 输出
		context.write(k3, new IntWritable(total));
	}

}
