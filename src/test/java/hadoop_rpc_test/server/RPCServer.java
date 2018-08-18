package hadoop_rpc_test.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer {

	public static void main(String[] args) throws Exception {
		// 定义一个RPC Builder
		RPC.Builder builder = new RPC.Builder(new Configuration());

		// 指定RPC Server的参数
		builder.setBindAddress("localhost");
		builder.setPort(7788);

		// 将自己的程序部署到Server上
		builder.setProtocol(MyInterface.class);
		builder.setInstance(new MyInterfaceImpl());

		// 创建Server
		Server server = builder.build();

		// 启动
		server.start();

	}

}
