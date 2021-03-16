// scalastyle:off
package org.apache.spark.rpc

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.config
import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage._
import org.apache.spark.{MapOutputTrackerMaster, SecurityManager, SparkConf, SparkContext}
import org.mockito.Mockito.spy
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class BlockManagerSuite extends AnyFunSuite {

	def makeBlockManager(): BlockManager = {
		val sparkConf = new SparkConf(false)

		sparkConf
			.set("spark.app.id", "test")
			.set("spark.testing", "true")
			.set("spark.memory.fraction", "1.0")
			.set("spark.memory.storageFraction", "0.999")

		val blockManagerConf = sparkConf
		blockManagerConf.set("spark.testing.memory", "1024000")

		val encryptionKey = None
		val securityManager = new SecurityManager(blockManagerConf, encryptionKey)

		val rpcEnv = RpcEnv.create("test",
			sparkConf.get(config.DRIVER_HOST_ADDRESS),
			sparkConf.get(config.DRIVER_PORT), sparkConf, securityManager)

		val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]()

		val liveListenerBus = spy(new LiveListenerBus(sparkConf))

		val broadcastManager = new BroadcastManager(true, sparkConf)
		val mapOutputTracker = new MapOutputTrackerMaster(sparkConf, broadcastManager, true)

		val master: BlockManagerMaster = spy(new BlockManagerMaster(
			rpcEnv.setupEndpoint("blockmanager",
				new BlockManagerMasterEndpoint(rpcEnv, true, sparkConf, liveListenerBus, None,
					blockManagerInfo, mapOutputTracker)),
			rpcEnv.setupEndpoint("blockmanagerHeartbeat",
				new BlockManagerMasterHeartbeatEndpoint(rpcEnv, true, blockManagerInfo)), sparkConf,
			true)
		)

		val serializer = new KryoSerializer(blockManagerConf)

		val shuffleManager = new SortShuffleManager(sparkConf)

		val transferService = new NettyBlockTransferService(sparkConf,
			securityManager, "localhost", "localhost", 0, 1)

		val memoryManager = UnifiedMemoryManager(blockManagerConf, 1)

		val serializerManager = new SerializerManager(serializer, blockManagerConf)

		val externalShuffleClient = None

		val name = SparkContext.DRIVER_IDENTIFIER

		val blockManager = new BlockManager(name, rpcEnv, master,
			serializerManager, blockManagerConf, memoryManager, mapOutputTracker,
			shuffleManager, transferService, securityManager, externalShuffleClient)
		memoryManager.setMemoryStore(blockManager.memoryStore)

		blockManager.initialize("app-id")
		blockManager
	}


	test("block") {
		val blockManager = makeBlockManager()
		val a1 = new Array[Byte](4000)
		val a1TestBlockId = TestBlockId("a1")
		blockManager.putSingle(a1TestBlockId, a1, StorageLevel.MEMORY_ONLY)
	}



}
