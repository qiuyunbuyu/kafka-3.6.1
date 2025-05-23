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

package kafka.admin

import java.time.{Duration, Instant}
import java.util.{Collections, Properties}
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, Node, TopicPartition}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, immutable, mutable}
import scala.util.{Failure, Success, Try}
import joptsimple.{OptionException, OptionSpec}
import org.apache.kafka.common.protocol.Errors

import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.requests.ListOffsetsResponse

object ConsumerGroupCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new ConsumerGroupCommandOptions(args)
    try {
      opts.checkArgs()
      CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.")

      // should have exactly one action
      val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).count(opts.options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets")

      run(opts)
    } catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
    }
  }
  // the entry of "kafka-consumer-groups.sh"
  def run(opts: ConsumerGroupCommandOptions): Unit = {
    val consumerGroupService = new ConsumerGroupService(opts)
    try {
      if (opts.options.has(opts.listOpt)) {
        // --list
        consumerGroupService.listGroups()
      } else if (opts.options.has(opts.describeOpt)) {
        // --describe: describe group入口
        // the entry of "describeGroups"
        consumerGroupService.describeGroups()
      } else if (opts.options.has(opts.deleteOpt)) {
        // --delete
        consumerGroupService.deleteGroups()
      } else if (opts.options.has(opts.resetOffsetsOpt)) {
        // --reset-offsets：reset offset 入口
        val offsetsToReset = consumerGroupService.resetOffsets()
        // Export operation execution to a CSV file. Supported operations: reset-offsets.
        if (opts.options.has(opts.exportOpt)) {
          val exported = consumerGroupService.exportOffsetsToCsv(offsetsToReset)
          println(exported)
        } else
          printOffsetsToReset(offsetsToReset)
      }
      else if (opts.options.has(opts.deleteOffsetsOpt)) {
        // delete-offsets
        consumerGroupService.deleteOffsets()
      }
    } catch {
      case e: IllegalArgumentException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
      case e: Throwable =>
        printError(s"Executing consumer group command failed due to ${e.getMessage}", Some(e))
    } finally {
      consumerGroupService.close()
    }
  }

  def consumerGroupStatesFromString(input: String): Set[ConsumerGroupState] = {
    val parsedStates = input.split(',').map(s => ConsumerGroupState.parse(s.trim)).toSet
    if (parsedStates.contains(ConsumerGroupState.UNKNOWN)) {
      val validStates = ConsumerGroupState.values().filter(_ != ConsumerGroupState.UNKNOWN)
      throw new IllegalArgumentException(s"Invalid state list '$input'. Valid states are: ${validStates.mkString(", ")}")
    }
    parsedStates
  }

  val MISSING_COLUMN_VALUE = "-"

  def printError(msg: String, e: Option[Throwable] = None): Unit = {
    println(s"\nError: $msg")
    e.foreach(_.printStackTrace())
  }

  def printOffsetsToReset(groupAssignmentsToReset: Map[String, Map[TopicPartition, OffsetAndMetadata]]): Unit = {
    if (groupAssignmentsToReset.nonEmpty)
      println("\n%-30s %-30s %-10s %-15s".format("GROUP", "TOPIC", "PARTITION", "NEW-OFFSET"))
    for {
      (groupId, assignment) <- groupAssignmentsToReset
      (consumerAssignment, offsetAndMetadata) <- assignment
    } {
      println("%-30s %-30s %-10s %-15s".format(
        groupId,
        consumerAssignment.topic,
        consumerAssignment.partition,
        offsetAndMetadata.offset))
    }
  }

  private[admin] case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  private[admin] case class MemberAssignmentState(group: String, consumerId: String, host: String, clientId: String, groupInstanceId: String,
                                             numPartitions: Int, assignment: List[TopicPartition])

  private[admin] case class GroupState(group: String, coordinator: Node, assignmentStrategy: String, state: String, numMembers: Int)

  private[admin] sealed trait CsvRecord
  private[admin] case class CsvRecordWithGroup(group: String, topic: String, partition: Int, offset: Long) extends CsvRecord
  private[admin] case class CsvRecordNoGroup(topic: String, partition: Int, offset: Long) extends CsvRecord
  private[admin] object CsvRecordWithGroup {
    val fields = Array("group", "topic", "partition", "offset")
  }
  private[admin] object CsvRecordNoGroup {
    val fields = Array("topic", "partition", "offset")
  }
  // Example: CsvUtils().readerFor[CsvRecordWithoutGroup]
  private[admin] case class CsvUtils() {
    val mapper = new CsvMapper
    mapper.registerModule(DefaultScalaModule)
    def readerFor[T <: CsvRecord : ClassTag] = {
      val schema = getSchema[T]
      val clazz = implicitly[ClassTag[T]].runtimeClass
      mapper.readerFor(clazz).`with`(schema)
    }
    def writerFor[T <: CsvRecord : ClassTag] = {
      val schema = getSchema[T]
      val clazz = implicitly[ClassTag[T]].runtimeClass
      mapper.writerFor(clazz).`with`(schema)
    }
    private def getSchema[T <: CsvRecord : ClassTag] = {
      val clazz = implicitly[ClassTag[T]].runtimeClass

      val fields =
        if (classOf[CsvRecordWithGroup] == clazz) CsvRecordWithGroup.fields
        else if (classOf[CsvRecordNoGroup] == clazz) CsvRecordNoGroup.fields
        else throw new IllegalStateException(s"Unhandled class $clazz")

      val schema = mapper.schemaFor(clazz).sortedBy(fields: _*)
      schema
    }
  }

  class ConsumerGroupService(val opts: ConsumerGroupCommandOptions,
                             private[admin] val configOverrides: Map[String, String] = Map.empty) {

    private val adminClient = createAdminClient(configOverrides)

    // We have to make sure it is evaluated once and available
    private lazy val resetPlanFromFile: Option[Map[String, Map[TopicPartition, OffsetAndMetadata]]] = {
      if (opts.options.has(opts.resetFromFileOpt)) {
        val resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt)
        val resetPlanCsv = Utils.readFileAsString(resetPlanPath)
        val resetPlan = parseResetPlan(resetPlanCsv)
        Some(resetPlan)
      } else None
    }

    def listGroups(): Unit = {
      if (opts.options.has(opts.stateOpt)) {
        val stateValue = opts.options.valueOf(opts.stateOpt)
        val states = if (stateValue == null || stateValue.isEmpty)
          Set[ConsumerGroupState]()
        else
          consumerGroupStatesFromString(stateValue)
        val listings = listConsumerGroupsWithState(states)
        printGroupStates(listings.map(e => (e.groupId, e.state.get.toString)))
      } else
        listConsumerGroups().foreach(println(_))
    }

    def listConsumerGroups(): List[String] = {
      val result = adminClient.listConsumerGroups(withTimeoutMs(new ListConsumerGroupsOptions))
      val listings = result.all.get.asScala
      listings.map(_.groupId).toList
    }

    def listConsumerGroupsWithState(states: Set[ConsumerGroupState]): List[ConsumerGroupListing] = {
      val listConsumerGroupsOptions = withTimeoutMs(new ListConsumerGroupsOptions())
      listConsumerGroupsOptions.inStates(states.asJava)
      val result = adminClient.listConsumerGroups(listConsumerGroupsOptions)
      result.all.get.asScala.toList
    }

    private def printGroupStates(groupsAndStates: List[(String, String)]): Unit = {
      // find proper columns width
      var maxGroupLen = 15
      for ((groupId, _) <- groupsAndStates) {
        maxGroupLen = Math.max(maxGroupLen, groupId.length)
      }
      println(s"%${-maxGroupLen}s %s".format("GROUP", "STATE"))
      for ((groupId, state) <- groupsAndStates) {
        println(s"%${-maxGroupLen}s %s".format(groupId, state))
      }
    }

    private def shouldPrintMemberState(group: String, state: Option[String], numRows: Option[Int]): Boolean = {
      // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
      // if it's undefined or 0, there is no relevant group information to display.
      numRows match {
        case None =>
          printError(s"The consumer group '$group' does not exist.")
          false
        case Some(num) => state match {
          case Some("Dead") =>
            printError(s"Consumer group '$group' does not exist.")
          case Some("Empty") =>
            Console.err.println(s"\nConsumer group '$group' has no active members.")
          case Some("PreparingRebalance") | Some("CompletingRebalance") =>
            Console.err.println(s"\nWarning: Consumer group '$group' is rebalancing.")
          case Some("Stable") =>
          case other =>
            // the control should never reach here
            throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
        }
        !state.contains("Dead") && num > 0
      }
    }

    private def size(colOpt: Option[Seq[Object]]): Option[Int] = colOpt.map(_.size)

    private def printOffsets(offsets: Map[String, (Option[String], Option[Seq[PartitionAssignmentState]])]): Unit = {
      for ((groupId, (state, assignments)) <- offsets) {
        if (shouldPrintMemberState(groupId, state, size(assignments))) {
          // find proper columns width
          var (maxGroupLen, maxTopicLen, maxConsumerIdLen, maxHostLen) = (15, 15, 15, 15)
          assignments match {
            case None => // do nothing
            case Some(consumerAssignments) =>
              consumerAssignments.foreach { consumerAssignment =>
                maxGroupLen = Math.max(maxGroupLen, consumerAssignment.group.length)
                maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE).length)
                maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE).length)
                maxHostLen = Math.max(maxHostLen, consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE).length)
              }
          }

          println(s"\n%${-maxGroupLen}s %${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s"
            .format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"))

          assignments match {
            case None => // do nothing
            case Some(consumerAssignments) =>
              consumerAssignments.foreach { consumerAssignment =>
                println(s"%${-maxGroupLen}s %${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s".format(
                  consumerAssignment.group,
                  consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE))
                )
              }
          }
        }
      }
    }

    private def printMembers(members: Map[String, (Option[String], Option[Seq[MemberAssignmentState]])], verbose: Boolean): Unit = {
      for ((groupId, (state, assignments)) <- members) {
        if (shouldPrintMemberState(groupId, state, size(assignments))) {
          // find proper columns width
          var (maxGroupLen, maxConsumerIdLen, maxGroupInstanceIdLen, maxHostLen, maxClientIdLen, includeGroupInstanceId) = (15, 15, 17, 15, 15, false)
          assignments match {
            case None => // do nothing
            case Some(memberAssignments) =>
              memberAssignments.foreach { memberAssignment =>
                maxGroupLen = Math.max(maxGroupLen, memberAssignment.group.length)
                maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length)
                maxGroupInstanceIdLen =  Math.max(maxGroupInstanceIdLen, memberAssignment.groupInstanceId.length)
                maxHostLen = Math.max(maxHostLen, memberAssignment.host.length)
                maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length)
                includeGroupInstanceId = includeGroupInstanceId || memberAssignment.groupInstanceId.nonEmpty
              }
          }

          if (includeGroupInstanceId) {
            print(s"\n%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxGroupInstanceIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
                .format("GROUP", "CONSUMER-ID", "GROUP-INSTANCE-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
          } else {
            print(s"\n%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
                .format("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
          }
          if (verbose)
            print(s"%s".format("ASSIGNMENT"))
          println()

          assignments match {
            case None => // do nothing
            case Some(memberAssignments) =>
              memberAssignments.foreach { memberAssignment =>
                if (includeGroupInstanceId) {
                  print(s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxGroupInstanceIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s ".format(
                    memberAssignment.group, memberAssignment.consumerId, memberAssignment.groupInstanceId, memberAssignment.host,
                    memberAssignment.clientId, memberAssignment.numPartitions))
                } else {
                  print(s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s ".format(
                    memberAssignment.group, memberAssignment.consumerId, memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions))
                }
                if (verbose) {
                  val partitions = memberAssignment.assignment match {
                    case List() => MISSING_COLUMN_VALUE
                    case assignment =>
                      assignment.groupBy(_.topic).map {
                        case (topic, partitionList) => topic + partitionList.map(_.partition).sorted.mkString("(", ",", ")")
                      }.toList.sorted.mkString(", ")
                  }
                  print(s"%s".format(partitions))
                }
                println()
              }
          }
        }
      }
    }

    private def printStates(states: Map[String, GroupState]): Unit = {
      for ((groupId, state) <- states) {
        if (shouldPrintMemberState(groupId, Some(state.state), Some(1))) {
          val coordinator = s"${state.coordinator.host}:${state.coordinator.port} (${state.coordinator.idString})"
          val coordinatorColLen = Math.max(25, coordinator.length)
          print(s"\n%${-coordinatorColLen}s %-25s %-20s %-15s %s".format("GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS"))
          print(s"\n%${-coordinatorColLen}s %-25s %-20s %-15s %s".format(state.group, coordinator, state.assignmentStrategy, state.state, state.numMembers))
          println()
        }
      }
    }
    /* three/four requests to collect
    * (--all-groups). ListGroupsRequest
    * a. DescribeGroups Request
    * b. OffsetFetch Request
    * c. ListOffsets Request
    */
    def describeGroups(): Unit = {
      // 1. get params
      val groupIds =
        // all-groups : send ListGroupsRequest to get "groupIds" first
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        // --group group1
        else opts.options.valuesOf(opts.groupOpt).asScala
      // --members
      val membersOptPresent = opts.options.has(opts.membersOpt)
      // --state
      val stateOptPresent = opts.options.has(opts.stateOpt)
      // --offsets
      val offsetsOptPresent = opts.options.has(opts.offsetsOpt)
      val subActions = Seq(membersOptPresent, offsetsOptPresent, stateOptPresent).count(_ == true)

      if (subActions == 0 || offsetsOptPresent) {
        val offsets = collectGroupsOffsets(groupIds)
        printOffsets(offsets)
      } else if (membersOptPresent) {
        val members = collectGroupsMembers(groupIds, opts.options.has(opts.verboseOpt))
        printMembers(members, opts.options.has(opts.verboseOpt))
      } else {
        val states = collectGroupsState(groupIds)
        printStates(states)
      }
    }

    private def collectConsumerAssignment(group: String,
                                          coordinator: Option[Node],
                                          topicPartitions: Seq[TopicPartition],
                                          getPartitionOffset: TopicPartition => Option[Long],
                                          consumerIdOpt: Option[String],
                                          hostOpt: Option[String],
                                          clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {
      if (topicPartitions.isEmpty) {
        Array[PartitionAssignmentState](
          PartitionAssignmentState(group, coordinator, None, None, None, getLag(None, None), consumerIdOpt, hostOpt, clientIdOpt, None)
        )
      }
      else
        describePartitions(group, coordinator, topicPartitions.sortBy(_.partition), getPartitionOffset, consumerIdOpt, hostOpt, clientIdOpt)
    }

    /**
     * --describe consumer group时会打印出 “lag” 的 数值
     * @param commitedOffset
     * @param logEndOffset
     * @return "lag"
     */
    private def getLag(offset: Option[Long], logEndOffset: Option[Long]): Option[Long] =
      offset.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))

    private def describePartitions(group: String,
                                   coordinator: Option[Node],
                                   topicPartitions: Seq[TopicPartition],
                                   getPartitionOffset: TopicPartition => Option[Long],
                                   consumerIdOpt: Option[String],
                                   hostOpt: Option[String],
                                   clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {

      def getDescribePartitionResult(topicPartition: TopicPartition, logEndOffsetOpt: Option[Long]): PartitionAssignmentState = {
        // use commited Offset and LEO to calculate Lag
        val offset = getPartitionOffset(topicPartition)
        PartitionAssignmentState(group, coordinator, Option(topicPartition.topic), Option(topicPartition.partition), offset,
          getLag(offset, logEndOffsetOpt), consumerIdOpt, hostOpt, clientIdOpt, logEndOffsetOpt)
      }
      // try get LEO
      getLogEndOffsets(group, topicPartitions).map {
        logEndOffsetResult =>
          logEndOffsetResult._2 match {
            case LogOffsetResult.LogOffset(logEndOffset) => getDescribePartitionResult(logEndOffsetResult._1, Some(logEndOffset))
            case LogOffsetResult.Unknown => getDescribePartitionResult(logEndOffsetResult._1, None)
            case LogOffsetResult.Ignore => null
          }
      }.toArray
    }

    def resetOffsets(): Map[String, Map[TopicPartition, OffsetAndMetadata]] = {
      // 1. reset offset 用户输入是基于Groups的, 返回 groupIds:List[String] 列表
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups() // --all-groups，利用 ListGroupsRequest: get all groupIds
        else opts.options.valuesOf(opts.groupOpt).asScala // --group 用户指定了

      // 2. 使用 [DescribeGroupsRequest] ，获取Map[String, KafkaFuture[ConsumerGroupDescription]]
      // key: group ID + value: 此Consumer group 对应的信息(ConsumerGroupDescription)，包括组内成员等
      val consumerGroups = adminClient.describeConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups()

      val result =
        consumerGroups.asScala.foldLeft(immutable.Map[String, Map[TopicPartition, OffsetAndMetadata]]()) {
          // acc：累加器，用于存储之前处理的消费者组的结果
          case (acc, (groupId, groupDescription)) =>
            groupDescription.get.state().toString match {
              // 只有 Consumer Group 是"Empty" | "Dead"才可以重置
              case "Empty" | "Dead" =>
                // 1. 获取此ConsumerGroup下想要重置的Seq[TopicPartition]
                val partitionsToReset = getPartitionsToReset(groupId)

                //  2. 根据reset情况
                //  返回 TopicPartition 对应的想要重置的 OffsetAndMetadata
                //  支持如下重置模式：
                //      resetToOffsetOpt, resetShiftByOpt,
                //      resetToDatetimeOpt, resetByDurationOpt,
                //      resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt
                //      resetFromFileOpt
                val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)

                // Dry-run is the default behavior if --execute is not specified
                val dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt)
                if (!dryRun) {
                  // 3. call adminClient to send "OffsetCommitRequest"
                  // 实际就是利用OffsetCommitRequest，把步骤2中计算出的 OffsetAndMetadata，提交给Consumer Coordinator存储
                  adminClient.alterConsumerGroupOffsets(
                    groupId,
                    preparedOffsets.asJava,
                    withTimeoutMs(new AlterConsumerGroupOffsetsOptions)
                  ).all.get
                }
                acc.updated(groupId, preparedOffsets)
              case currentState =>
                // 非inactive的Consumer Group 无法重置
                printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")
                acc.updated(groupId, Map.empty)
            }
        }
      result
    }

    def deleteOffsets(groupId: String, topics: List[String]): (Errors, Map[TopicPartition, Throwable]) = {
      val partitionLevelResult = mutable.Map[TopicPartition, Throwable]()

      val (topicWithPartitions, topicWithoutPartitions) = topics.partition(_.contains(":"))
      val knownPartitions = topicWithPartitions.flatMap(parseTopicsWithPartitions)

      // Get the partitions of topics that the user did not explicitly specify the partitions
      // call MetaDataRequest
      val describeTopicsResult = adminClient.describeTopics(
        topicWithoutPartitions.asJava,
        withTimeoutMs(new DescribeTopicsOptions))

      val unknownPartitions = describeTopicsResult.topicNameValues().asScala.flatMap { case (topic, future) =>
        Try(future.get()) match {
          case Success(description) => description.partitions().asScala.map { partition =>
            new TopicPartition(topic, partition.partition())
          }
          case Failure(e) =>
            partitionLevelResult += new TopicPartition(topic, -1) -> e
            List.empty
        }
      }

      val partitions = knownPartitions ++ unknownPartitions
      // call OffsetDeleteRequest
      val deleteResult = adminClient.deleteConsumerGroupOffsets(
        groupId,
        partitions.toSet.asJava,
        withTimeoutMs(new DeleteConsumerGroupOffsetsOptions)
      )

      var topLevelException = Errors.NONE
      Try(deleteResult.all.get) match {
        case Success(_) =>
        case Failure(e) => topLevelException = Errors.forException(e.getCause)
      }

      partitions.foreach { partition =>
        Try(deleteResult.partitionResult(partition).get()) match {
          case Success(_) => partitionLevelResult += partition -> null
          case Failure(e) => partitionLevelResult += partition -> e
        }
      }

      (topLevelException, partitionLevelResult)
    }

    def deleteOffsets(): Unit = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val topics = opts.options.valuesOf(opts.topicOpt).asScala.toList

      val (topLevelResult, partitionLevelResult) = deleteOffsets(groupId, topics)

      topLevelResult match {
        case Errors.NONE =>
          println(s"Request succeed for deleting offsets with topic ${topics.mkString(", ")} group $groupId")
        case Errors.INVALID_GROUP_ID =>
          printError(s"'$groupId' is not valid.")
        case Errors.GROUP_ID_NOT_FOUND =>
          printError(s"'$groupId' does not exist.")
        case Errors.GROUP_AUTHORIZATION_FAILED =>
          printError(s"Access to '$groupId' is not authorized.")
        case Errors.NON_EMPTY_GROUP =>
          printError(s"Deleting offsets of a consumer group '$groupId' is forbidden if the group is not empty.")
        case Errors.GROUP_SUBSCRIBED_TO_TOPIC |
             Errors.TOPIC_AUTHORIZATION_FAILED |
             Errors.UNKNOWN_TOPIC_OR_PARTITION =>
          printError(s"Encounter some partition level error, see the follow-up details:")
        case _ =>
          printError(s"Encounter some unknown error: $topLevelResult")
      }

      println("\n%-30s %-15s %-15s".format("TOPIC", "PARTITION", "STATUS"))
      partitionLevelResult.toList.sortBy(t => t._1.topic + t._1.partition.toString).foreach { case (tp, error) =>
        println("%-30s %-15s %-15s".format(
          tp.topic,
          if (tp.partition >= 0) tp.partition else "Not Provided",
          if (error != null) s"Error: ${error.getMessage}" else "Successful"
        ))
      }
    }

    private[admin] def describeConsumerGroups(groupIds: Seq[String]): mutable.Map[String, ConsumerGroupDescription] = {
      adminClient.describeConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups().asScala.map {
        case (groupId, groupDescriptionFuture) => (groupId, groupDescriptionFuture.get())
      }
    }

    /**
      * Returns the state of the specified consumer group and partition assignment states
      */
    def collectGroupOffsets(groupId: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      collectGroupsOffsets(List(groupId)).getOrElse(groupId, (None, None))
    }

    /**
      * Returns states of the specified consumer groups and partition assignment states
      * three requests to collect
      * 1. DescribeGroups Request
      * 2. OffsetFetch Request
      * 3. ListOffsets Request
      */
    def collectGroupsOffsets(groupIds: Seq[String]): TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])] = {
      // DescribeGroupsRequest | get [String, ConsumerGroupDescription]
      val consumerGroups = describeConsumerGroups(groupIds)

      val groupOffsets = TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state
        // OffsetFetch Request | try get committedOffsets
        val committedOffsets = getCommittedOffsets(groupId)

        // The admin client returns `null` as a value to indicate that there is not committed offset for a partition.
        // 定义了一个用于获取某个TopicPartition的committedOffsets的函数，这个函数会作为后面很多方法的入参
        def getPartitionOffset(tp: TopicPartition): Option[Long] = committedOffsets.get(tp).filter(_ != null).map(_.offset)

        var assignedTopicPartitions = ListBuffer[TopicPartition]()

        // get rowsWithConsumer
        val rowsWithConsumer = consumerGroup.members.asScala.filterNot(_.assignment.topicPartitions.isEmpty).toSeq
          .sortBy(_.assignment.topicPartitions.size)(Ordering[Int].reverse).flatMap { consumerSummary =>
          val topicPartitions = consumerSummary.assignment.topicPartitions.asScala
          assignedTopicPartitions = assignedTopicPartitions ++ topicPartitions
          // ListOffsetsRequest | [ get LEO and calculate lag]
          collectConsumerAssignment(groupId, Option(consumerGroup.coordinator), topicPartitions.toList,
            getPartitionOffset, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
            Some(s"${consumerSummary.clientId}"))
        }

        val unassignedPartitions = committedOffsets.filterNot { case (tp, _) => assignedTopicPartitions.contains(tp) }

        // rowsWithoutConsumer
        val rowsWithoutConsumer = if (unassignedPartitions.nonEmpty) {
          collectConsumerAssignment(
            groupId,
            Option(consumerGroup.coordinator),
            unassignedPartitions.keySet.toSeq,
            getPartitionOffset,
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE)).toSeq
        } else
          Seq.empty

        groupId -> (Some(state.toString), Some(rowsWithConsumer ++ rowsWithoutConsumer))
      }).toMap

      groupOffsets
    }

    private[admin] def collectGroupMembers(groupId: String, verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = {
      collectGroupsMembers(Seq(groupId), verbose)(groupId)
    }

    private[admin] def collectGroupsMembers(groupIds: Seq[String], verbose: Boolean): TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state.toString
        val memberAssignmentStates = consumerGroup.members().asScala.map(consumer =>
          MemberAssignmentState(
            groupId,
            consumer.consumerId,
            consumer.host,
            consumer.clientId,
            consumer.groupInstanceId.orElse(""),
            consumer.assignment.topicPartitions.size(),
            if (verbose) consumer.assignment.topicPartitions.asScala.toList else List()
          )).toList
        groupId -> (Some(state), Option(memberAssignmentStates))
      }).toMap
    }

    private[admin] def collectGroupState(groupId: String): GroupState = {
      collectGroupsState(Seq(groupId))(groupId)
    }

    private[admin] def collectGroupsState(groupIds: Seq[String]): TreeMap[String, GroupState] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, GroupState]() ++ (for ((groupId, groupDescription) <- consumerGroups) yield {
        groupId -> GroupState(
          groupId,
          groupDescription.coordinator,
          groupDescription.partitionAssignor(),
          groupDescription.state.toString,
          groupDescription.members().size
        )
      }).toMap
    }

    private def getLogEndOffsets(groupId: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val endOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.latest
      }.toMap
      // get Leo by ListOffsetsRequest
      val offsets = adminClient.listOffsets(
        endOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogStartOffsets(groupId: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val startOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.earliest
      }.toMap
      val offsets = adminClient.listOffsets(
        startOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogTimestampOffsets(groupId: String, topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
      val timestampOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.forTimestamp(timestamp)
      }.toMap
      val offsets = adminClient.listOffsets(
        timestampOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
        offsets.asScala.partition(_._2.offset != ListOffsetsResponse.UNKNOWN_OFFSET)

      val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
        case (topicPartition, listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
      }.toMap

      unsuccessfulOffsetsForTimes.foreach { entry =>
        println(s"\nWarn: Partition " + entry._1.partition() + " from topic " + entry._1.topic() +
          " is empty. Falling back to latest known offset.")
      }

      successfulLogTimestampOffsets ++ getLogEndOffsets(groupId, unsuccessfulOffsetsForTimes.keySet.toSeq)
    }

    def close(): Unit = {
      adminClient.close()
    }

    // Visibility for testing
    protected def createAdminClient(configOverrides: Map[String, String]): Admin = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      configOverrides.forKeyValue { (k, v) => props.put(k, v)}
      Admin.create(props)
    }

    private def withTimeoutMs [T <: AbstractOptions[T]] (options : T) =  {
      val t = opts.options.valueOf(opts.timeoutMsOpt).intValue()
      options.timeoutMs(t)
    }

    private def parseTopicsWithPartitions(topicArg: String): Seq[TopicPartition] = {
      def partitionNum(partition: String): Int = {
        try {
          partition.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"Invalid partition '$partition' specified in topic arg '$topicArg''")
        }
      }
      topicArg.split(":") match {
        case Array(topic, partitions) =>
          partitions.split(",").map(partition => new TopicPartition(topic, partitionNum(partition)))
        case _ =>
          throw new IllegalArgumentException(s"Invalid topic arg '$topicArg', expected topic name and partitions")
      }
    }

    /**
     * @param topicArgs 用户指定了要重置的topicArgs
     * @return 确定要重置的Seq[TopicPartition]
     */
    private def parseTopicPartitionsToReset(topicArgs: Seq[String]): Seq[TopicPartition] = {
      val (topicsWithPartitions, topics) = topicArgs.partition(_.contains(":"))
      // 既指定了topic也指定了partition，直接从文本中解析出即可
      val specifiedPartitions = topicsWithPartitions.flatMap(parseTopicsWithPartitions)

      // 只指定了topic，未指定具体分区，通过MetadataRequest来获取topic的Partitions信息
      val unspecifiedPartitions = if (topics.nonEmpty) {
        val descriptionMap = adminClient.describeTopics(
          topics.asJava,
          withTimeoutMs(new DescribeTopicsOptions)
        ).allTopicNames().get.asScala
        descriptionMap.flatMap { case (topic, description) =>
          description.partitions().asScala.map { tpInfo =>
            new TopicPartition(topic, tpInfo.partition)
          }
        }
      } else
        Seq.empty
      // 最终返回整体
      specifiedPartitions ++ unspecifiedPartitions
    }

    // 实际执行reset操作的，一定是Consumer Group -> TopicPartition-X
    // 但是 TopicPartition-X 是基于 Topic 而言的，
    // 所以这里就是根据 --all-topics 和 --topic，来获取涉及的 TopicPartition-X
    private def getPartitionsToReset(groupId: String): Seq[TopicPartition] = {
      // --all-topics场景：这个哈意思呢？我就要重置Consumer Group下面所有的TopicPartition，我也不知道涉及哪些Topic的情况
      if (opts.options.has(opts.allTopicsOpt)) {
        getCommittedOffsets(groupId).keys.toSeq
      } else if (opts.options.has(opts.topicOpt)) {
      // --topic场景：指定了具体的Topic
        val topics = opts.options.valuesOf(opts.topicOpt).asScala
        parseTopicPartitionsToReset(topics)
      } else {
      // --from-file
        if (opts.options.has(opts.resetFromFileOpt))
          Nil
        else
          ToolsUtils.printUsageAndExit(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.")
      }
    }

    /**
     * FindCoordinatorRequest + OffsetFetchRequest
     * @param groupId: 一个 Conusmer Group
     * @return 对应的Conusmer Group下的current OffsetAndMetadata：Map[TopicPartition, OffsetAndMetadata]
     */
    private def getCommittedOffsets(groupId: String): Map[TopicPartition, OffsetAndMetadata] = {
      adminClient.listConsumerGroupOffsets(
        Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec),
        withTimeoutMs(new ListConsumerGroupOffsetsOptions())
      ).partitionsToOffsetAndMetadata(groupId).get().asScala
    }

    type GroupMetadata = immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]
    private def parseResetPlan(resetPlanCsv: String): GroupMetadata = {
      def updateGroupMetadata(group: String, topic: String, partition: Int, offset: Long, acc: GroupMetadata) = {
        val topicPartition = new TopicPartition(topic, partition)
        val offsetAndMetadata = new OffsetAndMetadata(offset)
        val dataMap = acc.getOrElse(group, immutable.Map()).updated(topicPartition, offsetAndMetadata)
        acc.updated(group, dataMap)
      }
      val csvReader = CsvUtils().readerFor[CsvRecordNoGroup]
      val lines = resetPlanCsv.split("\n")
      val isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1
      val isOldCsvFormat = lines.headOption.flatMap(line =>
        Try(csvReader.readValue[CsvRecordNoGroup](line)).toOption).nonEmpty
      // Single group CSV format: "topic,partition,offset"
      val dataMap = if (isSingleGroupQuery && isOldCsvFormat) {
        val group = opts.options.valueOf(opts.groupOpt)
        lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
          val CsvRecordNoGroup(topic, partition, offset) = csvReader.readValue[CsvRecordNoGroup](line)
          updateGroupMetadata(group, topic, partition, offset, acc)
        }
        // Multiple group CSV format: "group,topic,partition,offset"
      } else {
        val csvReader = CsvUtils().readerFor[CsvRecordWithGroup]
        lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
          val CsvRecordWithGroup(group, topic, partition, offset) = csvReader.readValue[CsvRecordWithGroup](line)
          updateGroupMetadata(group, topic, partition, offset, acc)
        }
      }
      dataMap
    }

    /**
     * 从这里就可以看出为啥重置Offset需要 Consumer Group是Inactive？
     * 因为rest之前需要获取当前的情况，然后按情况进行一个计算
     * 如果Consumer Group是active的，那就意味着当前Consumer Offset是不稳定的, 自然也就无法计算出一个准确的 reset Consumer Offset值
     *
     * @param groupId 要重置的 groupId
     * @param partitionsToReset 更进一步 要重置的 groupId 下的 Seq[TopicPartition]
     * @return 确定这个 TopicPartition 对应要重置的 OffsetAndMetadata
     */
    private def prepareOffsetsToReset(groupId: String,
                                      partitionsToReset: Seq[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
      // [ case1: --to-offset情况 ]
      if (opts.options.has(opts.resetToOffsetOpt)) {
        // 取到--to-offset后面的值
        val offset = opts.options.valueOf(opts.resetToOffsetOpt)
        // 校验这个 --to-offset后面的值 是不是 在每个partition 合理的范围内(logStartOffsets, logEndOffsets)
        checkOffsetsRange(groupId, partitionsToReset.map((_, offset)).toMap).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToEarliestOpt)) {
      // [ case2: --to-earliest情况 ]
      // Map[TopicPartition, LogOffsetResult]: TopicPartition对应的logStartOffset
        val logStartOffsets = getLogStartOffsets(groupId, partitionsToReset)
        partitionsToReset.map { topicPartition =>
          logStartOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting starting offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetToLatestOpt)) {
        // [ case3: to-latest情况 ]
        // Map[TopicPartition, LogOffsetResult]: TopicPartition对应的logEndOffset
        val logEndOffsets = getLogEndOffsets(groupId, partitionsToReset)
        partitionsToReset.map { topicPartition =>
          logEndOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetShiftByOpt)) {
        // [ case4: shift-by情况 ]
        // 1. 先获取groupId下面每个TopicPartition对应的已提交的OffsetAndMetadata，Map[TopicPartition, OffsetAndMetadata]
        val currentCommittedOffsets = getCommittedOffsets(groupId)
        // 2. 根据shift-by的值 以及现有的值，计算出requestedOffsets(currentOffset + shiftBy)
        val requestedOffsets = partitionsToReset.map { topicPartition =>
          val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
          val currentOffset = currentCommittedOffsets.getOrElse(topicPartition,
            throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset")).offset
          (topicPartition, currentOffset + shiftBy)
        }.toMap
        // 3. 校验requestedOffsets是否在合理范围
        checkOffsetsRange(groupId, requestedOffsets).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToDatetimeOpt)) {
        // [ case5: to-datetime情况 ]
        // 将字符串形式的时间转换成long型的timestamp
        val timestamp = Utils.getDateTime(opts.options.valueOf(opts.resetToDatetimeOpt))
        // ListOffsetsRequest指定对应timestamp，获取对应的offset
        val logTimestampOffsets = getLogTimestampOffsets(groupId, partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetByDurationOpt)) {
        // [ case6: by-duration情况 ]：Reset offsets to offset by duration from current timestamp
        val duration = opts.options.valueOf(opts.resetByDurationOpt)
        val durationParsed = Duration.parse(duration)
        val now = Instant.now()
        durationParsed.negated().addTo(now)
        // 最终还是转换成了目标时间戳 timestamp
        val timestamp = now.minus(durationParsed).toEpochMilli
        val logTimestampOffsets = getLogTimestampOffsets(groupId, partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (resetPlanFromFile.isDefined) {
        // [ case7: 从csv中解析的情况 ]
        resetPlanFromFile.map(resetPlan => resetPlan.get(groupId).map { resetPlanForGroup =>
          val requestedOffsets = resetPlanForGroup.keySet.map { topicPartition =>
            topicPartition -> resetPlanForGroup(topicPartition).offset
          }.toMap
          checkOffsetsRange(groupId, requestedOffsets).map {
            case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
          }
        } match {
          case Some(resetPlanForGroup) => resetPlanForGroup
          case None =>
            printError(s"No reset plan for group $groupId found")
            Map[TopicPartition, OffsetAndMetadata]()
        }).getOrElse(Map.empty)
      } else if (opts.options.has(opts.resetToCurrentOpt)) {
        // [ case7: to-current的情况 ]
        // 1. 先获取对应的Conusmer Group下的current OffsetAndMetadata
        val currentCommittedOffsets = getCommittedOffsets(groupId)

        // 2. 将currentCommittedOffsets划分成 2个 结果集(有CommittedOffset，没有CommittedOffset)
        val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
          partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))

        // 3. 有CommittedOffset -> 现有CommittedOffset
        val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
          (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
            case Some(offset) => offset.offset
            case None => throw new IllegalStateException(s"Expected a valid current offset for topic partition: $topicPartition")
          }))
        }.toMap

        // 4. 没有CommittedOffset -> 获取LogEndOffset
        val preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(groupId, partitionsToResetWithoutCommittedOffset).map {
          case (topicPartition, LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
          case (topicPartition, _) => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
        }

        // 5. 组装结果集
        preparedOffsetsForPartitionsWithCommittedOffset ++ preparedOffsetsForPartitionsWithoutCommittedOffset
      } else {
        ToolsUtils.printUsageAndExit(opts.parser, "Option '%s' requires one of the following scenarios: %s".format(opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts))
      }
    }

    private def checkOffsetsRange(groupId: String, requestedOffsets: Map[TopicPartition, Long]) = {
      val logStartOffsets = getLogStartOffsets(groupId, requestedOffsets.keySet.toSeq)
      val logEndOffsets = getLogEndOffsets(groupId, requestedOffsets.keySet.toSeq)
      requestedOffsets.map { case (topicPartition, offset) => (topicPartition,
        logEndOffsets.get(topicPartition) match {
          case Some(LogOffsetResult.LogOffset(endOffset)) if offset > endOffset =>
            warn(s"New offset ($offset) is higher than latest offset for topic partition $topicPartition. Value will be set to $endOffset")
            endOffset

          case Some(_) => logStartOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(startOffset)) if offset < startOffset =>
              warn(s"New offset ($offset) is lower than earliest offset for topic partition $topicPartition. Value will be set to $startOffset")
              startOffset

            case _ => offset
          }

          case None => // the control should not reach here
            throw new IllegalStateException(s"Unexpected non-existing offset value for topic partition $topicPartition")
        })
      }
    }

    def exportOffsetsToCsv(assignments: Map[String, Map[TopicPartition, OffsetAndMetadata]]): String = {
      val isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1
      val csvWriter =
        if (isSingleGroupQuery) CsvUtils().writerFor[CsvRecordNoGroup]
        else CsvUtils().writerFor[CsvRecordWithGroup]
      val rows = assignments.flatMap { case (groupId, partitionInfo) =>
        partitionInfo.map { case (k: TopicPartition, v: OffsetAndMetadata) =>
          val csvRecord =
            if (isSingleGroupQuery) CsvRecordNoGroup(k.topic, k.partition, v.offset)
            else CsvRecordWithGroup(groupId, k.topic, k.partition, v.offset)
          csvWriter.writeValueAsString(csvRecord)
        }
      }
      rows.mkString("")
    }

    def deleteGroups(): Map[String, Throwable] = {
      // all or specify
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        else opts.options.valuesOf(opts.groupOpt).asScala
      // call AdminClient to send "DeleteGroupsRequest"
      val groupsToDelete = adminClient.deleteConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DeleteConsumerGroupsOptions)
      ).deletedGroups().asScala

      val result = groupsToDelete.map { case (g, f) =>
        Try(f.get) match {
          case Success(_) => g -> null
          case Failure(e) => g -> e
        }
      }

      val (success, failed) = result.partition {
        case (_, error) => error == null
      }

      if (failed.isEmpty) {
        println(s"Deletion of requested consumer groups (${success.keySet.mkString("'", "', '", "'")}) was successful.")
      }
      else {
        printError("Deletion of some consumer groups failed:")
        failed.foreach {
          case (group, error) => println(s"* Group '$group' could not be deleted due to: ${error.toString}")
        }
        if (success.nonEmpty)
          println(s"\nThese consumer groups were deleted successfully: ${success.keySet.mkString("'", "', '", "'")}")
      }

      result.toMap
    }
  }

  sealed trait LogOffsetResult

  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server(s) to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted or topic whose should be included in the reset offset process. " +
      "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
      "Reset-offsets also supports multiple topic inputs."
    val AllTopicsDoc = "Consider all topics assigned to a group in the `reset-offsets` process."
    val ListDoc = "List all consumer groups."
    val DescribeDoc = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group."
    val AllGroupsDoc = "Apply to all consumer groups."
    val nl = System.getProperty("line.separator")
    val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2"
    val TimeoutMsDoc = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
      "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
      "or is going through some changes)."
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."

    // Rest Offset 相关文档 ↓ 其他地方还有相关的
    val ResetOffsetsDoc = "Reset offsets of consumer group. Supports one consumer group at the time, and instances should be inactive" + nl +
      "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to update the offsets. " +
      "Additionally, the --export option is used to export the results to a CSV format." + nl +
      "You must choose one of the following reset specifications: --to-datetime, --by-duration, --to-earliest, " +
      "--to-latest, --shift-by, --from-file, --to-current, --to-offset." + nl +
      "To define the scope use --all-topics or --topic. One scope must be specified unless you use '--from-file'."
    val DryRunDoc = "Only show results without executing changes on Consumer Groups. Supported operations: reset-offsets."
    val ExecuteDoc = "Execute operation. Supported operations: reset-offsets."
    val ExportDoc = "Export operation execution to a CSV file. Supported operations: reset-offsets."
    val ResetToOffsetDoc = "Reset offsets to a specific offset."
    val ResetFromFileDoc = "Reset offsets to values defined in CSV file."
    val ResetToDatetimeDoc = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'"
    val ResetByDurationDoc = "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'"
    val ResetToEarliestDoc = "Reset offsets to earliest offset."
    val ResetToLatestDoc = "Reset offsets to latest offset."
    val ResetToCurrentDoc = "Reset offsets to current offset."
    val ResetShiftByDoc = "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative."
    // Rest Offset 相关文档 ↑

    val MembersDoc = "Describe members of the group. This option may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --members"
    val VerboseDoc = "Provide additional information, if any, when describing the group. This option may be used " +
      "with '--offsets'/'--members'/'--state' and '--bootstrap-server' options only." + nl + "Example: --bootstrap-server localhost:9092 --describe --group group1 --members --verbose"
    val OffsetsDoc = "Describe the group and list all topic partitions in the group along with their offset lag. " +
      "This is the default sub-action of and may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --offsets"
    val StateDoc = "When specified with '--describe', includes the state of the group." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --state" + nl +
      "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states." + nl +
      "Example: --bootstrap-server localhost:9092 --list --state stable,empty" + nl +
      "This option may be used with '--describe', '--list' and '--bootstrap-server' options only."
    val DeleteOffsetsDoc = "Delete offsets of consumer group. Supports one consumer group at the time, and multiple topics."

    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server to connect to")
                                   .ofType(classOf[String])
    val groupOpt = parser.accepts("group", GroupDoc)
                         .withRequiredArg
                         .describedAs("consumer group")
                         .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val allTopicsOpt = parser.accepts("all-topics", AllTopicsDoc)
    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val allGroupsOpt = parser.accepts("all-groups", AllGroupsDoc)
    val deleteOpt = parser.accepts("delete", DeleteDoc)
    val timeoutMsOpt = parser.accepts("timeout", TimeoutMsDoc)
                             .withRequiredArg
                             .describedAs("timeout (ms)")
                             .ofType(classOf[Long])
                             .defaultsTo(5000)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                  .withRequiredArg
                                  .describedAs("command config property file")
                                  .ofType(classOf[String])
    val resetOffsetsOpt = parser.accepts("reset-offsets", ResetOffsetsDoc)
    val deleteOffsetsOpt = parser.accepts("delete-offsets", DeleteOffsetsDoc)
    val dryRunOpt = parser.accepts("dry-run", DryRunDoc)
    val executeOpt = parser.accepts("execute", ExecuteDoc)
    val exportOpt = parser.accepts("export", ExportDoc)
    val resetToOffsetOpt = parser.accepts("to-offset", ResetToOffsetDoc)
                           .withRequiredArg()
                           .describedAs("offset")
                           .ofType(classOf[Long])
    val resetFromFileOpt = parser.accepts("from-file", ResetFromFileDoc)
                                 .withRequiredArg()
                                 .describedAs("path to CSV file")
                                 .ofType(classOf[String])
    val resetToDatetimeOpt = parser.accepts("to-datetime", ResetToDatetimeDoc)
                                   .withRequiredArg()
                                   .describedAs("datetime")
                                   .ofType(classOf[String])
    val resetByDurationOpt = parser.accepts("by-duration", ResetByDurationDoc)
                                   .withRequiredArg()
                                   .describedAs("duration")
                                   .ofType(classOf[String])
    val resetToEarliestOpt = parser.accepts("to-earliest", ResetToEarliestDoc)
    val resetToLatestOpt = parser.accepts("to-latest", ResetToLatestDoc)
    val resetToCurrentOpt = parser.accepts("to-current", ResetToCurrentDoc)
    val resetShiftByOpt = parser.accepts("shift-by", ResetShiftByDoc)
                             .withRequiredArg()
                             .describedAs("number-of-offsets")
                             .ofType(classOf[Long])
    val membersOpt = parser.accepts("members", MembersDoc)
                           .availableIf(describeOpt)
    val verboseOpt = parser.accepts("verbose", VerboseDoc)
                           .availableIf(describeOpt)
    val offsetsOpt = parser.accepts("offsets", OffsetsDoc)
                           .availableIf(describeOpt)
    val stateOpt = parser.accepts("state", StateDoc)
                         .availableIf(describeOpt, listOpt)
                         .withOptionalArg()
                         .ofType(classOf[String])

    options = parser.parse(args : _*)

    val allGroupSelectionScopeOpts = immutable.Set[OptionSpec[_]](groupOpt, allGroupsOpt)
    val allConsumerGroupLevelOpts = immutable.Set[OptionSpec[_]](listOpt, describeOpt, deleteOpt, resetOffsetsOpt)
    val allResetOffsetScenarioOpts = immutable.Set[OptionSpec[_]](resetToOffsetOpt, resetShiftByOpt,
      resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt)
    val allDeleteOffsetsOpts = immutable.Set[OptionSpec[_]](groupOpt, topicOpt)

    def checkArgs(): Unit = {

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(describeOpt)) {
        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        val mutuallyExclusiveOpts: Set[OptionSpec[_]] = Set(membersOpt, offsetsOpt, stateOpt)
        if (mutuallyExclusiveOpts.toList.map(o => if (options.has(o)) 1 else 0).sum > 1) {
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt takes at most one of these options: ${mutuallyExclusiveOpts.mkString(", ")}")
        }
        if (options.has(stateOpt) && options.valueOf(stateOpt) != null)
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt does not take a value for $stateOpt")
      } else {
        if (options.has(timeoutMsOpt))
          debug(s"Option $timeoutMsOpt is applicable only when $describeOpt is used.")
      }

      if (options.has(deleteOpt)) {
        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $deleteOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        if (options.has(topicOpt))
          CommandLineUtils.printUsageAndExit(parser, s"The consumer does not support topic-specific offset " +
            "deletion from a consumer group.")
      }

      if (options.has(deleteOffsetsOpt)) {
        if (!options.has(groupOpt) || !options.has(topicOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $deleteOffsetsOpt takes the following options: ${allDeleteOffsetsOpts.mkString(", ")}")
      }

      if (options.has(resetOffsetsOpt)) {
        if (options.has(dryRunOpt) && options.has(executeOpt))
          CommandLineUtils.printUsageAndExit(parser, s"Option $resetOffsetsOpt only accepts one of $executeOpt and $dryRunOpt")

        if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
          Console.err.println("WARN: No action will be performed as the --execute option is missing." +
            "In a future major release, the default behavior of this command will be to prompt the user before " +
            "executing the reset rather than doing a dry run. You should add the --dry-run option explicitly " +
            "if you are scripting this command and want to keep the current default behavior without prompting.")
        }

        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $resetOffsetsOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        CommandLineUtils.checkInvalidArgs(parser, options, resetToOffsetOpt,   (allResetOffsetScenarioOpts - resetToOffsetOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, (allResetOffsetScenarioOpts - resetToDatetimeOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetByDurationOpt, (allResetOffsetScenarioOpts - resetByDurationOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, (allResetOffsetScenarioOpts - resetToEarliestOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt,   (allResetOffsetScenarioOpts - resetToLatestOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToCurrentOpt,  (allResetOffsetScenarioOpts - resetToCurrentOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetShiftByOpt,    (allResetOffsetScenarioOpts - resetShiftByOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt,   (allResetOffsetScenarioOpts - resetFromFileOpt).asJava)
      }

      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allGroupSelectionScopeOpts - groupOpt).asJava)
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allConsumerGroupLevelOpts - describeOpt - deleteOpt - resetOffsetsOpt).asJava)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, (allConsumerGroupLevelOpts - deleteOpt - resetOffsetsOpt).asJava )
    }
  }
}
