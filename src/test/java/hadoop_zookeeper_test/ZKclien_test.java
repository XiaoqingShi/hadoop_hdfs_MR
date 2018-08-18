package hadoop_zookeeper_test;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/**
 * @author movw
 * @date 2018年8月17日 下午6:32:42
 * @version 1.0
 */
public class ZKclien_test {
	private static String connectString = "bigdata111:2181";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;

	// 创建子节点
	@Test
	public void create() throws Exception {
		// 数据的增删改查
		// 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
		String nodeCreated = zkClient.create("/eclipse", "hello zk".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
	}

	@Before
	public void init() throws Exception {

		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 收到事件通知后的回调函数（用户的业务逻辑）
				System.out.println(event.getType() + "--" + event.getPath());
				System.out.println(event.getWrapper() + "回调");
				// 再次启动监听
				try {
					zkClient.getChildren("/", true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	// 获取子节点
	@Test
	public void getChildren() throws Exception {
		List<String> children = zkClient.getChildren("/", true);

		for (String child : children) {
			System.out.println(child);
		}

		// 延时阻塞
		Thread.sleep(Long.MAX_VALUE);
	}

	// 判断znode是否存在
	@Test
	public void exist() throws Exception {
		Stat stat = zkClient.exists("/eclipse", false);

		System.out.println(stat == null ? "not exist" : "exist");
	}

}
