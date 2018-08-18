package hadoop_hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

/**
 * @author movw
 * @date 2018年8月16日 下午3:32:21
 * @version 1.0
 */
public class Test_hdfs_FileSystem {
	// configuration对象
	public static Configuration conf;
	// 静态代码块
	static {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://bigdata111:9000");
		conf.set("dfs.replication", "1");
	}

	/**
	 * 通过FileSystem 上传
	 * 
	 * @throws IOException
	 */
	@Test
	public void test_upload() throws IOException {
		// 获取 FileSystem 对象
		FileSystem fileSystem = FileSystem.get(conf);
		// 上传
		fileSystem.copyFromLocalFile(new Path("D:\\ITStar人工智能\\老尚家\\Hadoop\\1.笔记\\尚硅谷大数据技术之Hadoop（HDFS文件系统）.doc"),
				new Path("/a.doc"));
		// 关闭
		fileSystem.close();
		System.out.println("ok");
	}

	/**
	 * 通过FileSystem URI 上传
	 * 
	 * @throws IOException
	 */
	@Test
	public void test_upload1() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
		// 上传
		fileSystem.copyFromLocalFile(new Path("D:\\ITStar人工智能\\老尚家\\Hadoop\\1.笔记\\尚硅谷大数据技术之Hadoop（HDFS文件系统）.doc"),
				new Path("/a1.doc"));
		// 关闭
		fileSystem.close();
		System.out.println("ok");
	}

	/**
	 * 通过API下载
	 * 
	 * @throws Exception
	 */
	@Test
	public void getFileFromHDFS() throws Exception {

		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// fs.copyToLocalFile(new Path("hdfs://bigdata111:9000/a.doc"),
		// new Path("d:/hello.txt"));
		// boolean delSrc 指是否将原文件删除
		// Path src 指要下载的文件路径
		// Path dst 指将文件下载到的路径
		// boolean useRawLocalFileSystem 是否开启文件效验
		// 2 下载文件
		fs.copyToLocalFile(false, new Path("hdfs://bigdata111:9000/a.doc"), new Path("e:/hellocopy.doc"), true);
		fs.close();
		System.out.println("ok");
	}

	/**
	 * 目录创建
	 * 
	 * @throws Exception
	 */
	@Test
	public void mkdirAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 创建目录
		fs.mkdirs(new Path("hdfs://bigdata111:9000/user/root/output"));
		System.out.println("ok");
	}

	/**
	 * 文件夹删除
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 删除文件夹 ，如果是非空文件夹，参数2是否递归删除，true递归
		fs.delete(new Path("hdfs://bigdata111:9000/a1.doc"), true);
		System.out.println("ok");
	}

	/**
	 * 文件重命名
	 * 
	 * @throws Exception
	 */
	@Test
	public void renameAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 重命名文件或文件夹
		fs.rename(new Path("hdfs://bigdata111:9000/a.doc"), new Path("hdfs://bigdata111:9000/a2.doc"));
		System.out.println("ok");
	}

	/**
	 * 文件详情查看
	 * 
	 * @throws Exception
	 */
	@Test
	public void readListFiles() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 思考：为什么返回迭代器，而不是List之类的容器
		// 这是因为递归查询文件非常多，如果用list等容器，客户端压力很大，迭代器是建立与服务器之间的联系，不用把大量数据拉到客户端；例二查询的包括目录和文件，毕竟有限。
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();

			System.out.print(fileStatus.getPath().getName());
			System.out.print(fileStatus.getBlockSize());
			System.out.print(fileStatus.getPermission());
			System.out.println(fileStatus.getLen());

			BlockLocation[] blockLocations = fileStatus.getBlockLocations();

			for (BlockLocation bl : blockLocations) {

				System.out.println("block-offset:" + bl.getOffset());

				String[] hosts = bl.getHosts();

				for (String host : hosts) {
					System.out.println(host);
				}
			}

			System.out.println("--------------李冰冰的分割线--------------");
		}
	}

	/**
	 * 文件与文件夹的
	 * 
	 * @throws Exception
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void findAtHDFS() throws Exception, IllegalArgumentException, IOException {

		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

		// 2 获取查询路径下的文件状态信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		// 3 遍历所有文件状态
		for (FileStatus status : listStatus) {
			if (status.isFile()) {
				System.out.println("f--" + status.getPath().getName());
			} else {
				System.out.println("d--" + status.getPath().getName());
			}
		}
		System.out.println("ok");
	}

}
