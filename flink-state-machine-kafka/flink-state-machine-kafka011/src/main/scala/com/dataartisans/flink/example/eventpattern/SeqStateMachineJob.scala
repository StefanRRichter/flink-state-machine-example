/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.example.eventpattern

import java.util.Properties

import com.dataartisans.flink.example.eventpattern.kafka.{KafkaUtils, SeqEventDeSerializer}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
 * Demo streaming program that receives a stream of events and evaluates
 * those events w.r.t to an increasing sequence of long values.
 *
 * Basic invocation line:
 * --input-topic <>
 * --bootstrap.servers localhost:9092
 * --zookeeper.servers localhost:2181 (only needed for older Kafka versions)
 * --checkpointDir <>
 * --isolation.level <> (default is read_committed)
 * --semantic exactly-once (alternatives: at-least-once)
 *
 * StateBackend-related options:
 * --stateBackend: file or rocksdb (default: file)
 * --asyncCheckpoints: true or false (only file backend, default: false)
 * --incrementalCheckpoints: true or false (only RocksDB backend, default: false)
 * --externalizedCheckpoints: true or false (default: false)
 * --restartDelay: <int> (default: 0)
 * --checkpointInterval: <int in ms> (default: 5000)
 */
object SeqStateMachineJob {

  def main(args: Array[String]): Unit = {

    // retrieve input parameters
    val pt: ParameterTool = ParameterTool.fromArgs(args)

    if (pt.has("help")) {

      System.out.println(
        "  Demo streaming program that receives a stream of events and evaluates\n" +
          "  those events w.r.t to an increasing sequence of long values.\n" +
          " \n" +
          "  Basic invocation line:\n" +
          "  --input-topic <>\n" +
          "  --bootstrap.servers localhost:9092\n" +
          "  --zookeeper.servers localhost:2181 (only needed for older Kafka versions)\n" +
          "  --checkpointDir <>\n" +
          "  --isolation.level <> (default is read_committed)\n" +
          "  --semantic exactly-once (alternatives: at-least-once)\n" +
          " \n" +
          "  StateBackend-related options:\n" +
          "  --stateBackend: file or rocksdb (default: file)\n" +
          "  --asyncCheckpoints: true or false (only file backend, default: false)\n" +
          "  --incrementalCheckpoints: true or false (only RocksDB backend, default: false)\n" +
          "  --externalizedCheckpoints: true or false (default: false)\n" +
          "  --restartDelay: <int> (default: 0)\n" +
          "  --checkpointInterval: <int in ms> (default: 5000)")

      return
    }
    
    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(pt.getInt("checkpointInterval", 5000))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Int.MaxValue, pt.getInt("restartDelay", 0)))
    if (pt.has("externalizedCheckpoints") && pt.getBoolean("externalizedCheckpoints", false)) {
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    env.setParallelism(pt.getInt("parallelism", 1))
    env.setMaxParallelism(pt.getInt("maxParallelism", pt.getInt("parallelism", 1)))

    val stateBackend = pt.get("stateBackend", "file")
    val checkpointDir = pt.getRequired("checkpointDir")

    stateBackend match {
      case "file" =>
        val asyncCheckpoints = pt.getBoolean("asyncCheckpoints", false)
        env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints))
      case "rocksdb" =>
        val incrementalCheckpoints = pt.getBoolean("incrementalCheckpoints", false)
        env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints))
    }

    val stream = env.addSource(
      new FlinkKafkaConsumer011[SeqEvent](
        pt.getRequired("input-topic"),
        new SeqEventDeSerializer(),
        createKafkaConsumerProperties(pt.getProperties)))

    // Uncomment line below to use EventsGeneratorSource which does not require Kafka
    // val stream = env.addSource(new EventsGeneratorSource(true)).setParallelism(1)

    val semanticArg = pt.get("semantic", "exactly-once")

    val mapFun = if ("exactly-once".equalsIgnoreCase(semanticArg)) {
      new SeqExactlyOnceStateMachineMapper()
    } else if ("at-least-once".equalsIgnoreCase(semanticArg)) {
      new SeqAtLeastOnceStateMachineMapper()
    } else {
      throw new IllegalArgumentException("Semantic must be either exactly-once or at-least-once, was: " + semanticArg)
    }

    val alerts = stream
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .keyBy("sourceAddress")
      
      // the function that evaluates the state machine over the sequence of events
      .flatMap(mapFun)


    // if we get any alert, fail
    alerts.flatMap { any =>
      throw new RuntimeException(s"Got an alert: $any.")
      "Make type checker happy."
    }


    // trigger program execution
    env.execute()
  }

  /**
    * Creates a copy of the input containing only the relevant keys for the Kafka Consumer.
    */
  private def createKafkaConsumerProperties(properties: Properties): Properties = {
    KafkaUtils.copyKafkaProperties(
      properties,
      Map(
        "bootstrap.servers" -> null,
        "zookeeper.servers" -> null,
        "isolation.level" -> "read_committed"))
  }
}

class SeqAtLeastOnceStateMachineMapper extends RichFlatMapFunction[SeqEvent, SeqAlert] {

  private[this] var currentState: ValueState[Long] = _

  override def open(config: Configuration): Unit = {
    currentState = getRuntimeContext.getState(new ValueStateDescriptor("state", classOf[Long]))
  }

  override def flatMap(t: SeqEvent, out: Collector[SeqAlert]): Unit = {
    val value = Option(currentState.value()).getOrElse(0L)
    val nextVal = t.value

    if (nextVal > value) {
      if (nextVal == (value + 1L)) {
        currentState.update(nextVal)
      } else {
        out.collect(SeqAlert(t.sourceAddress, value, nextVal))
      }
    }
  }
}

class SeqExactlyOnceStateMachineMapper extends RichFlatMapFunction[SeqEvent, SeqAlert] {

  private[this] var currentState: ValueState[Long] = _

  override def open(config: Configuration): Unit = {
    currentState = getRuntimeContext.getState(new ValueStateDescriptor("state", classOf[Long]))
  }

  override def flatMap(t: SeqEvent, out: Collector[SeqAlert]): Unit = {
    val value = Option(currentState.value()).getOrElse(0L)
    val nextVal = t.value

      if (nextVal == (value + 1L)) {
        currentState.update(nextVal)
      } else {
        out.collect(SeqAlert(t.sourceAddress, value, nextVal))
      }
  }
}
