package hadoop_rpc_test.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import hadoop_rpc_test.server.MyInterface;

public class RPCClient {

	public static void main(String[] args) throws Exception {
		// 得到的是服务器端的一个代理对象
		MyInterface proxy = RPC.getProxy(MyInterface.class, // 调用服务器端的接口
				MyInterface.versionID, // 版本号
				new InetSocketAddress("localhost", 7788), // 指定RPC Server的地址
				new Configuration());

		String result = proxy.sayHello("Tom");
		System.out.println("结果是：" + result);
	}

}
