// scalastyle:off
package example

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.Log
import org.scalatest.funsuite.AnyFunSuite
import sun.misc.Unsafe

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

class BaseSuite extends AnyFunSuite with Log {


	test("unsafe") {
		val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
		unsafeField.setAccessible(true)
		val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
		val offset = unsafe.arrayBaseOffset(classOf[Array[Byte]])
		println(offset)
	}


	test("future") {

		val executorService = Executors.newFixedThreadPool(5)

		println(executorService)

		val future = executorService.submit(new Callable[Long] {
			override def call(): Long = {
				TimeUnit.SECONDS.sleep(10)
				System.currentTimeMillis()
			}
		})

		println(future.get())

	}

	case class U1()

	case class U2(name: String)

	object U1 {
		def unapply(u: U2): Option[(String, Seq[String])] = {
			Some(u.name, ListBuffer("1", "2"))
		}

		// 和apply 方法(构造函数)相反， 接收一个对象(任意的),从对象中提取值，
		// match 中 默认调用的是 case 中的unapply 方法来匹配( unapply 的参数，返回值)
		def unapply(arg: U1): Option[String] = {
			 Some("hello")
		}
	}

	test("unapply") {
		val u = U2("a")
		u match {
			// 隐式调用 unapply 方法
			case U1(x1, x2 @ immutable.List("1", "2")) => {
				println(x1, "list", x2.head)
			}
			// 可以通过返回值的类型判断case
			case U1(x1, x2 @ ListBuffer("1", "2")) => {
				println(x1, "list buffer", x2.head)
			}
			// U2 没有重写 unapply 方法，有默认的
			case U2(name) => {
				println(name)
			}
			case _ => println("other")
		}
	}
}
