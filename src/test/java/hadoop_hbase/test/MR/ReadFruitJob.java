package hadoop_hbase.test.MR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class ReadFruitJob {
	public static void main(String[] args) throws Exception {

		System.exit(run(args));
	}

	// 组装Job
	public static int run(String[] args) throws Exception {
		// 得到Configuration
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "172.16.145.111");

		// 创建Job任务
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFruitJob.class);
		// 配置Job
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		// 设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
		TableMapReduceUtil.initTableMapperJob("fruit", // 数据源的表名
				scan, // scan扫描控制器
				ReadFruitMapper.class, // 设置Mapper类
				ImmutableBytesWritable.class, // 设置Mapper输出key类型
				Put.class, // 设置Mapper输出value值类型
				job// 设置给哪个JOB
		);
		// 设置Reducer
		TableMapReduceUtil.initTableReducerJob("fruit_mr", ReadFruitReducr.class, job);
		// 设置Reduce数量，最少1个
		job.setNumReduceTasks(1);

		boolean isSuccess = job.waitForCompletion(true);
		if (!isSuccess) {
			throw new IOException("Job running with error");
		}
		return isSuccess ? 0 : 1;
	}

}
