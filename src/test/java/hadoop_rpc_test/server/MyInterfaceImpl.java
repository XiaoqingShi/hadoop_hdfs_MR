package hadoop_rpc_test.server;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class MyInterfaceImpl implements MyInterface {

	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		// 指定签名（版本号）
		return new ProtocolSignature(MyInterface.versionID, null);
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// 返回的该实现类的版本号
		return MyInterface.versionID;
	}

	@Override
	public String sayHello(String name) {
		System.out.println("********* 调用到了Server端*********");
		return "Hello " + name;
	}

}
