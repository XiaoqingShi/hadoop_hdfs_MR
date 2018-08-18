package hadoop_rpc_test.server;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol {

	// 定义一个版本号
	public static long versionID = 1;

	// 定义客户端可以调用的方法
	public String sayHello(String name);
}
