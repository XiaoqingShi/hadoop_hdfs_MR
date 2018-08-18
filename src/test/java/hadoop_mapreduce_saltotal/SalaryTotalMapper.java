package hadoop_mapreduce_saltotal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author movw
 * @date 2018年8月16日 下午6:25:22
 * @version 1.0
 */
public class SalaryTotalMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 获取一行
		String data = value.toString();
		String[] words = data.split(",");
		context.write(new IntWritable(Integer.parseInt(words[7])), // 部门号
				new IntWritable(Integer.parseInt(words[5])));// 薪水
	}

}
