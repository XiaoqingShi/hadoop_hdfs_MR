package jdbc_datasource;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class MyDataSourcePool implements DataSource {

	// 初始化连接池，放入10个链接
	private static String driver = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql://192.168.157.111:3306/hive";
	private static String user = "hiveowner";
	private static String password = "Welcome_1";

	// 定义一个链表来保存10个链接
	private static LinkedList<Connection> dataSource = new LinkedList<>();
	static {
		try {
			// 注册驱动
			Class.forName(driver);
			for (int i = 0; i < 10; i++) {
				dataSource.add(DriverManager.getConnection(url, user, password));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		// // 从连接池中返回一个连接
		// if(dataSource.size() > 0){
		// //有一个连接，直接返回该链接
		// return dataSource.removeFirst();
		// }else{
		// throw new SQLException("系统忙，请稍后.....");
		// }

		if (dataSource.size() > 0) {
			// 得到一个连接，是真正的对象
			Connection conn = dataSource.removeFirst();

			// 生成代理对象
			Connection proxy = (Connection) Proxy.newProxyInstance(MyDataSourcePool.class.getClassLoader(),
					conn.getClass().getInterfaces(), new InvocationHandler() {

						@Override
						public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
							// 重写close的逻辑
							if (method.getName().equals("close")) {
								// 将链接还给连接池
								dataSource.add(conn);
								System.out.println("链接已经还池.....");
								return null;
							} else {
								return method.invoke(conn, args);
							}
						}
					});

			// 返回代理对象
			return proxy;
		} else {
			throw new SQLException("系统忙，请稍后.....");
		}

	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getLoginTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
