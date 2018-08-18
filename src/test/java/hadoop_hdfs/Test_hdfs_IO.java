package hadoop_hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * 通过IO流操作HDFS
 * 
 * @author movw
 * @date 2018年8月16日 下午3:56:57
 * @version 1.0
 */
public class Test_hdfs_IO {
	/**
	 * 
	 * 通过IO上传
	 * 
	 * @throws Exception
	 */
	@Test
	public void putFileToHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 创建输入流
		FileInputStream inStream = new FileInputStream(new File("e:/hello.txt"));

		// 3 获取输出路径
		String putFileName = "hdfs://bigdata111:9000/user/root/hello1.txt";
		Path writePath = new Path(putFileName);

		// 4 创建输出流
		FSDataOutputStream outStream = fs.create(writePath);

		// 5 流对接
		try {
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
		}
	}

	/**
	 * 通过IO流下载
	 * 
	 * @throws Exception
	 */
	@Test
	public void getFileToHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 获取读取文件路径
		String filename = "hdfs://bigdata111:9000/a2.doc";

		// 3 创建读取path
		Path readPath = new Path(filename);

		// 4 创建输入流
		FSDataInputStream inStream = fs.open(readPath);
		// 创建输出流
		FileOutputStream outputStream = new FileOutputStream("e://a.doc");
		// 5 流对接输出到控制台
		try {
			IOUtils.copyBytes(inStream, outputStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}

	/**
	 * 定位下载第一块内容
	 * 
	 * @throws Exception
	 */
	@Test
	public void readFileSeek1() throws Exception {

		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "atguigu");

		// 2 获取输入流路径
		Path path = new Path("hdfs://bigdata111:9000/user/atguigu/tmp/hadoop-2.7.2.tar.gz");

		// 3 打开输入流
		FSDataInputStream fis = fs.open(path);

		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz.part1");

		// 5 流对接
		byte[] buf = new byte[1024];
		for (int i = 0; i < 128 * 1024; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		// 6 关闭流
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

	/**
	 * 定位下载第二块内容 合并文件 在window命令窗口中执行
	 * 
	 * type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
	 *
	 * 
	 * @throws Exception
	 */
	@Test
	public void readFileSeek2() throws Exception {

		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 获取输入流路径
		Path path = new Path("hdfs://hadoop102:9000/user/atguigu/tmp/hadoop-2.7.2.tar.gz");

		// 3 打开输入流
		FSDataInputStream fis = fs.open(path);

		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz.part2");

		// 5 定位偏移量（第二块的首位）
		fis.seek(1024 * 1024 * 128);

		// 6 流对接
		IOUtils.copyBytes(fis, fos, 1024);

		// 7 关闭流
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}

}
