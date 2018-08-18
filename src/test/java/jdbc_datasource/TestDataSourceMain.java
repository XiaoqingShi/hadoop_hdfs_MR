package jdbc_datasource;

import java.sql.Connection;

public class TestDataSourceMain {

	public static void main(String[] args) throws Exception {
		// 从连接池中获取链接 使用完后 释放链接
		MyDataSourcePool pool = new MyDataSourcePool();

		for (int i = 0; i < 11; i++) {
			Connection conn = pool.getConnection();// 得到的链接是真正的数据库链接

			System.out.println("第" + i + "个链接，是" + conn);

			conn.close(); // 把数据库的链接还给了数据库，而不是连接池
		}

	}

}
