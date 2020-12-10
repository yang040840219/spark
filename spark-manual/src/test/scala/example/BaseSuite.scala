// scalastyle:off
package example

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.Log
import org.scalatest.funsuite.AnyFunSuite
import sun.misc.Unsafe

class BaseSuite extends AnyFunSuite with Log{


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

		val future = executorService.submit(new Callable[Long]{
			override def call(): Long = {
					TimeUnit.SECONDS.sleep(10)
				  System.currentTimeMillis()
			}
		})

		println(future.get())

	}

}
