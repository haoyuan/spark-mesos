package spark

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.remote.RemoteActorRefProvider

import serializer.Serializer
import spark.broadcast.BroadcastManager
import spark.storage.BlockManager
import spark.storage.BlockManagerMaster
import spark.network.ConnectionManager
import spark.util.AkkaUtils

import tachyon.client.TachyonClient

/**
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 */
class SparkEnv (
    val executorId: String,
    val actorSystem: ActorSystem,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val tachyonSerializer: Serializer,
    val tachyonClient: TachyonClient
  ) {

  def stop() {
    httpFileServer.stop()
    mapOutputTracker.stop()
    shuffleFetcher.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    actorSystem.awaitTermination()
  }
}

object SparkEnv extends Logging {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean): SparkEnv = {

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, port)

    // Bit of a hack: If this is the driver and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.driver.port to it.
    if (isDriver && port == 0) {
      System.setProperty("spark.driver.port", boundPort.toString)
    }

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = System.getProperty(propertyName, defaultClassName)
      Class.forName(name, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializer = instantiateClass[Serializer]("spark.serializer", "spark.JavaSerializer")

    val driverIp: String = System.getProperty("spark.driver.host", "localhost")
    val driverPort: Int = System.getProperty("spark.driver.port", "7077").toInt
    val blockManagerMaster = new BlockManagerMaster(
      actorSystem, isDriver, isLocal, driverIp, driverPort)
    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster, serializer)

    val connectionManager = blockManager.connectionManager

    val broadcastManager = new BroadcastManager(isDriver)

    val closureSerializer = instantiateClass[Serializer](
      "spark.closure.serializer", "spark.JavaSerializer")

    val cacheManager = new CacheManager(blockManager)

    val mapOutputTracker = new MapOutputTracker(actorSystem, isDriver)

    val shuffleFetcher = instantiateClass[ShuffleFetcher](
      "spark.shuffle.fetcher", "spark.BlockStoreShuffleFetcher")

    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    System.setProperty("spark.fileserver.uri", httpFileServer.serverUri)

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir().getAbsolutePath
    } else {
      "."
    }

    // Warn about deprecated spark.cache.class property
    if (System.getProperty("spark.cache.class") != null) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }

    val tachyonAddress = System.getProperty("spark.tachyon.address")
    val tachyonSerializerClass =
      System.getProperty("spark.tachyon.serializer", "spark.JavaSerializer")
    val tachyonSerializer =
      Class.forName(tachyonSerializerClass).newInstance().asInstanceOf[Serializer]
    var tachyonClient: TachyonClient = null
    tachyonClient = TachyonClient.getClient(tachyonAddress)
    logWarning("TachyonClient has connected to " + tachyonAddress)

    new SparkEnv(
      executorId,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleFetcher,
      broadcastManager,
      blockManager,
      connectionManager,
      httpFileServer,
      sparkFilesDir,
      tachyonSerializer,
      tachyonClient)
  }
}
