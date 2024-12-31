/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer, Server}
import kafka.utils.Implicits._
import kafka.utils.{Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Time, Utils}
import org.apache.kafka.server.util.CommandLineUtils

object Kafka extends Logging {

  /**
   * @param args: server.properties or other
   * @return Properties
   */
  def getPropsFromArgs(args: Array[String]): Properties = {
    // 1. define OptionParser to handle args
    val optionParser = new OptionParser(false)

    // 2. --override override values set in server.properties file
    // first get from server.properties file, if have "--override" Options, will override values set in server.properties file
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    // 3. --version handle
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    // 4. if no args or --help
    if (args.isEmpty || args.contains("--help")) {
      CommandLineUtils.printUsageAndExit(optionParser,
        "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
    }

    //5. print Version And Exit
    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndExit()
    }

    // * 6. load Props in server.properties and assign to props
    val props = Utils.loadProps(args(0))

    // 7. Other parameter processing
    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndExit(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }
      // add other props in CommandLine to props(defined in *6)
      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt))
    }
    props
  }

  // For Zk mode, the API forwarding is currently enabled only under migration flag. We can
  // directly do a static IBP check to see API forwarding is enabled here because IBP check is
  // static in Zk mode.
  private def enableApiForwarding(config: KafkaConfig) =
    config.migrationEnabled && config.interBrokerProtocolVersion.isApiForwardingEnabled

  /**
   * build kafka-server from Properties
   * zk模式 -> KafkaServer
   * raft模式 -> KafkaRaftServer
   * @param props
   * @return
   */
  private def buildServer(props: Properties): Server = {
    // 1. build KafkaConfig from props
    val config = KafkaConfig.fromProps(props, false)
    // 2. determine which startup mode, the judgment standard is whether contains the parameter process.role
    if (config.requiresZookeeper) {
      // 2.1 zk mode
      new KafkaServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None,
        enableForwarding = enableApiForwarding(config)
      )
    } else {
      // 2.2 raft mode
      new KafkaRaftServer(
        config,
        Time.SYSTEM,
      )
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      // 1. get props
      val serverProps = getPropsFromArgs(args)

      // 2. build server from props：会确定以什么模式启动： zk/raft
      val server = buildServer(serverProps)

      // 3. LoggingSignalHandler
      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // 4. attach shutdown handler to catch terminating signals as well as normal termination
      //    will call server.shutdown() before shutdown
      Exit.addShutdownHook("kafka-shutdown-hook", {
        // 正常关闭时(接收到TERM信号)，触发关闭前的相关”清理“工作
        // [PENDING_CONTROLLED_SHUTDOWN -> SHUTTING_DOWN -> NOT_RUNNING]
        // Shutdown: shutdownLatch - 1
        try server.shutdown()
        catch {
          case _: Throwable =>
            fatal("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Exit.halt(1)
        }
      })

      // 5. start server: [NOT_RUNNING -> STARTING -> RECOVERY -> RUNNING]
      // 如果是zk模式，专注于KafkaServer即可
      // start: shutdownLatch = 1
      try server.startup()
      catch {
        case e: Throwable =>
          // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
          fatal("Exiting Kafka due to fatal exception during startup.", e)
          Exit.exit(1)
      }

      // 6. wait server shutdown
      // shutdownLatch = 0 -> exit!
      server.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    // 7. Exit normally
    Exit.exit(0)
  }
}
