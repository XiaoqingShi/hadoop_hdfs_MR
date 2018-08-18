package hadoop_mapreduce_wordcount2_Partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author movw
 * @date 2018年8月16日 下午5:54:06
 * @version 1.0
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// 获取单词
		String firWord = key.toString();
		char[] charArray = firWord.toCharArray();
		int result = charArray[0];
		// 奇数
		if (result % 2 == 0) {
			return 0;
		} else
			return 1;
	}

}
