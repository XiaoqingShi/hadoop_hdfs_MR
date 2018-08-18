package proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyTestMain {

	public static void main(String[] args) {
		// 创建真正的对象
		MyBusiness obj = new MyBusinessImpl();

		// 重写method1的实现 ---> 不修改源码
		// 生成真正对象的代理对象
		/*
		 * Proxy.newProxyInstance(loader, 类加载器 interfaces, 真正对象实现的接口 h )
		 * InvocationHandler 表示客户端如何调用代理对象
		 */

		MyBusiness proxyObj = (MyBusiness) Proxy.newProxyInstance(ProxyTestMain.class.getClassLoader(),
				obj.getClass().getInterfaces(), new InvocationHandler() {

					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						// 客户端的一次调用
						/*
						 * method: 客户端调用方法名 args : 方法的参数
						 */
						if (method.getName().equals("method1")) {
							// 重写
							System.out.println("******重写了method1*********");
							return null;
						} else {
							// 不感兴趣的方法 直接调用真正的对象完成
							return method.invoke(obj, args);
						}
					}
				});

		// 通过代理对象调用 method1 method2
		proxyObj.method1();
		proxyObj.method2();
	}

}
