// scalastyle:off
package org.apache.spark.rpc

import org.apache.spark.SparkConf
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.SecurityManager

class RpcSuite extends AnyFunSuite {

	val sparkConf = new SparkConf(false)

	val securityManager = new SecurityManager(sparkConf)


	test("server") {
		val config = RpcEnvConfig(sparkConf, "server", "localhost", "localhost", 19999, securityManager,
			0, clientMode = false)
		val env = new NettyRpcEnvFactory().create(config)
		println(env.address)

		var remoteRef: RpcEndpointRef = null
		env.setupEndpoint("server-endpoint", new RpcEndpoint {

			override val rpcEnv = env

			override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
				case Register(ref) =>
					remoteRef = ref
					context.reply("okay")
				case msg: String =>
					println("receive ......")
					context.reply(msg)
			}

			override def receive: PartialFunction[Any, Unit] = {
				case msg: String => {
					println(s"receive: $msg")
					// 调用client 端的receive 方法
					remoteRef.send("hello")
				}
				case _ => {
					println("other message type")
				}
			}

		})

		env.awaitTermination()
	}

	test("client") {
		val config = RpcEnvConfig(sparkConf, "client", "localhost", "localhost", 19998, securityManager,
			0, clientMode = false)
		val env = new NettyRpcEnvFactory().create(config)
		val serverAddress = RpcAddress.apply("localhost", 19999)
		val serverEndpointRef = env.setupEndpointRef(serverAddress, "server-endpoint")
		val clientEndpointRef = env.setupEndpoint("client-endpoint", new RpcEndpoint {
			override val rpcEnv: RpcEnv = env

			override def receive: PartialFunction[Any, Unit] = {
				case msg: String => {
					println(s"msg: $msg")
				}
				case _ => {
					println("other message type")
				}
			}
		})
		val reply = serverEndpointRef.askSync[String](Register(clientEndpointRef))
		assert("okay" === reply)
		serverEndpointRef.send("hello")

		env.awaitTermination()
	}
}
