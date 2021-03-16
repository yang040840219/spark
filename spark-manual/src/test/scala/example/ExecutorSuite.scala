// scalastyle:off
package example

import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite

class ExecutorSuite extends AnyFunSuite{
  
  
  test("main stop") {
	val executor = Executors.newFixedThreadPool(3)
	executor.submit(new Runnable {
	  override def run(): Unit = {
		TimeUnit.SECONDS.sleep(10)
		println(s"${Thread.currentThread().getName} run done")
	  }
	})
	println("main thread done")
  }
  
}
