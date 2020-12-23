// scalastyle:off
package org.apache.spark.sql.test

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Print
import org.apache.spark.util.UninterruptibleThread
import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito
import org.mockito.Mockito.{mock, spy, when}


class BaseSuite extends SparkFunSuite {


	test("uninterrupt thread") {
		val unIterruptThread = new UninterruptibleThread("uniterrupt") {
			override def run(): Unit = {
				runUninterruptibly({
					while (true) {
						println("hello!!!!!")
						TimeUnit.SECONDS.sleep(1)
						println("world!!!!!")
					}
				})
			}
		}

		unIterruptThread.start()
		Thread.sleep(3000)
		unIterruptThread.interrupt()
		println(unIterruptThread.isInterrupted)

	}


	test("interrupt thread") {
		val interrupt = new Thread("thread") {
			override def run() = {
				while (true) {
					println("hello !!!!!!")
					TimeUnit.SECONDS.sleep(1)
					println("world!!!!!")
				}
			}
		}

		interrupt.start()
		Thread.sleep(3000)
		interrupt.interrupt()

	}

	test("properties") {
		val s = System.getProperty("sun.net.client.defaultConnectTimeout")
		println(s)
	}

	test("compile star") {
		val pattern = Pattern.compile("\\*")
		Print.printConsole(pattern)
	}

	case class Form(name:String)

	class LoginService {
		def login(name:String): Boolean = {
			println("login .....")
			if("a".equals(name)){
				true
			} else {
				false
			}
		}

		def service(): String = {
			println("service ....")
			"real service"
		}

		def login(form:Form): Boolean = {
			if("a".equals(form.name)){
				true
			} else {
				false
			}
		}
	}

	test("mock") {
		val mockLoginService = mock(classOf[LoginService])

		// mock 产生的实例，无法调用其真实的方法, 返回值为null
		println(mockLoginService.service())

		val spyLoginService = spy(new LoginService)

		// spy 产生的实例, 可以执行其真实的方法
		// println(spyLoginService.service())

		when(mockLoginService.login("m")).thenReturn(true)

//		when(spyLoginService.service()).thenReturn("spy login service ....")
//
//		println(spyLoginService.service())

		Mockito.doReturn("spy login service ....", Nil: _*).when(spyLoginService).service()

		println(spyLoginService.service())

//
//		println(mockLoginService.login("a"))
//
//		val form = Form("a")
//		when(mockLoginService.login(meq(form))).thenReturn(true)
//		println(mockLoginService.login(form))

	}
}
