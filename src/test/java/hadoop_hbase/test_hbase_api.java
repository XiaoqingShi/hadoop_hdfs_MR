package hadoop_hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 测试HBASE API
 * 
 * @author movw
 * @date 0808
 */
public class test_hbase_api {
	// 指定ZooKeeper地址，从zk中获取HMaster的地址
	// 注意：ZK返回的是HMaster的主机名, 不是IP地址 ---> 配置Windows的hosts文件
	// C:\Windows\System32\drivers\etc\hosts
	public static Configuration conf;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.145.111");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws Exception {
		// 1
		// 表是否存在
		// System.out.println(isTableExist("student"));

		// 2
		// String[] columnFamily = { "info", "score" };
		// createTable("student1", columnFamily);

		// 3 删除表
		// deleteTable("student1");

		// 4 插入单个数据
		// String tableName = "student1";
		// String rowKey = "1001";
		// String columnFamily = "info";
		// String column = "age";
		// String value = "18";
		// insertRowData(tableName, rowKey, columnFamily, column, value);

		// 5 插入多个数据
		// String tableName = "student1";
		// String rowKey = "1002";
		// String columnFamily = "info";
		// String column = "age";
		// String value = "18";
		// insertRowDatas(tableName, rowKey, columnFamily, column, value);
		testGet();

	}

	/**
	 * 是否存在表
	 * 
	 * @param tableName表名
	 * @return boolean
	 * @throws IOException
	 */
	public static boolean isTableExist(String tableName) throws IOException {
		// HBaseAdmin admin = new HBaseAdmin(conf);//旧的API
		Connection connection = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		return admin.tableExists(tableName);
	}

	/**
	 * 创建表
	 * 
	 * @param tableName
	 *            表名
	 * @param columnFamily
	 *            列族
	 * @throws IOException
	 */
	public static void createTable(String tableName, String... columnFamily) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		// 表是否存在
		if (isTableExist(tableName)) {
			System.out.println("表已存在");
		} else {
			// 创建表属性对象，表名需要转字节
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			// 创建多个列族
			for (String cf : columnFamily) {
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			admin.createTable(descriptor);
			System.out.println("表：" + tableName + "创建成功！");
		}
	}

	/**
	 * 删除表
	 * 
	 * @param tableName
	 *            表名
	 * @throws IOException
	 */
	public static void deleteTable(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		if (isTableExist(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("表:" + tableName + "已删除");
		} else {
			System.out.println("表:" + tableName + "不存在");
		}
	}

	/**
	 * 插入单个数据
	 * 
	 * @param tableName
	 * @param rowKey
	 *            key
	 * @param columnFamily列族
	 * @param column列
	 * @param value值
	 * @throws IOException
	 */
	public static void insertRowData(String tableName, String rowKey, String columnFamily, String column, String value)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		// 创建HTable对象
		// HTable hTable = new HTable(conf, tableName);
		Table table = connection.getTable(TableName.valueOf(tableName));
		// 向表中插入数据
		Put put = new Put(Bytes.toBytes(rowKey));
		// put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
		// Bytes.toBytes(value));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		table.close();
		System.out.println("插入成功");
	}

	/**
	 * 插入多个数据
	 * 
	 * @param tableName
	 * @param rowKey
	 *            key
	 * @param columnFamily列族
	 * @param column列
	 * @param value值
	 * @throws IOException
	 */
	public static void insertRowDatas(String tableName, String rowKey, String columnFamily, String column, String value)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		// 创建HTable对象
		// HTable hTable = new HTable(conf, tableName);
		Table table = connection.getTable(TableName.valueOf(tableName));
		List<Put> puts = new ArrayList<Put>();
		// 向表中插入数据
		for (int i = 0; i < 3; i++) {
			Put put = new Put(Bytes.toBytes(rowKey));
			// put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
			// Bytes.toBytes(value));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column + i), Bytes.toBytes(value + i));
			puts.add(put);
		}
		table.put(puts);
		table.close();
		System.out.println("插入成功");
	}

	/**
	 * 获取表的所有数据
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void scanAllRow(String tableName) throws IOException {
		HTable hTable = new HTable(conf, tableName);
		// 得到用于扫描region的对象
		Scan scan = new Scan();
		// 使用HTable得到resultcanner实现类的对象
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				// 得到rowkey
				System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
				// 得到列族
				System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
	}

	/**
	 * 查询数据
	 * 
	 * @throws Exception
	 */
	public static void testGet() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "172.16.145.111");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf("student"));
		Get get = new Get(Bytes.toBytes("1001"));
		Result result = table.get(get);
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
		table.close();

	}
}
