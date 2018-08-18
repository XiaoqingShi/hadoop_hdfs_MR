package hadoop_hbase.test.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseFilter {

	public static void main(String[] args) throws Exception {
		// testSingleColumnValueFilter();
		// testColumnPrefixFilter();
		// testMultipleColumnPrefixFilter();
		// testRowFilter();
		testMoreFilter();
	}

	private static void testMoreFilter() throws Exception {
		/*
		 * 查询员工号是7839的员工姓名 select ename from emp where empno=7839; 1、rowkey过滤器 ：查询7839
		 * 2、列名前缀过滤器：查询姓名
		 */

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.157.111");

		// 创建表的客户端
		HTable emp = new HTable(conf, "emp");

		// 第一个过滤器
		RowFilter filter1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("7839"));

		// 第二个过滤器
		ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("ename"));

		// 创建一个Filter的list
		/*
		 * Operator.MUST_PASS_ALL 相当于 and Operator.MUST_PASS_ONE 相当于 or
		 */
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);

		// 创建Scanner
		Scan scanner = new Scan();
		scanner.setFilter(filterList);

		// 执行查询
		ResultScanner rs = emp.getScanner(scanner);
		for (Result r : rs) {
			String ename = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
			System.out.println(ename);
		}

		emp.close();
	}

	private static void testRowFilter() throws Exception {
		/*
		 * 查询行键为7839的员工薪水 select * from emp where empno=7839;
		 */
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.157.111");

		// 创建表的客户端
		HTable emp = new HTable(conf, "emp");

		// 构造一个过滤器 这个可以是一个正则表达式
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("7839"));
		Scan scanner = new Scan();
		scanner.setFilter(filter);

		// 执行查询
		ResultScanner rs = emp.getScanner(scanner);
		for (Result r : rs) {
			String ename = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
			String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));
			System.out.println(ename + "\t" + sal);
		}

		emp.close();
	}

	private static void testMultipleColumnPrefixFilter() throws Exception {
		/*
		 * 查询员工的姓名和薪水：select ename,sal from emp;
		 */

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.157.111");

		// 创建表的客户端
		HTable emp = new HTable(conf, "emp");

		// 构造一个二维数据，代表每个列的名字
		byte[][] prefixes = new byte[][] { Bytes.toBytes("ename"), Bytes.toBytes("sal") };

		// 定义多个列名前缀过滤器
		MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(prefixes);

		Scan scanner = new Scan();
		scanner.setFilter(filter);

		// 执行查询
		ResultScanner rs = emp.getScanner(scanner);
		for (Result r : rs) {
			String ename = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
			String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));
			System.out.println(ename + "\t" + sal);
		}

		emp.close();

	}

	private static void testColumnPrefixFilter() throws Exception {
		/*
		 * 查询员工的姓名：select ename from emp;
		 */
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.157.111");

		// 创建表的客户端
		HTable emp = new HTable(conf, "emp");

		// 定义一个过滤器
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("ename"));

		Scan scanner = new Scan();
		scanner.setFilter(filter);

		// 执行查询
		ResultScanner rs = emp.getScanner(scanner);
		for (Result r : rs) {
			String ename = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
			System.out.println(ename);
		}

		emp.close();
	}

	// 列值过滤器： SingleColumnValueFilter
	public static void testSingleColumnValueFilter() throws Exception {
		/*
		 * 查询薪水等于3000的员工 select * from emp where sal=3000;
		 */
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.157.111");

		// 创建表的客户端
		HTable emp = new HTable(conf, "emp");

		// 创建过滤器
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("empinfo"), // 列族
				Bytes.toBytes("sal"), // 列名
				CompareOp.EQUAL, // 比较运算符
				Bytes.toBytes("3000")); // 值

		// 创建一个Scanner
		Scan scanner = new Scan();
		scanner.setFilter(filter);

		// 执行查询
		ResultScanner rs = emp.getScanner(scanner);
		for (Result r : rs) {
			String ename = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
			System.out.println(ename);
		}

		emp.close();
	}

}
