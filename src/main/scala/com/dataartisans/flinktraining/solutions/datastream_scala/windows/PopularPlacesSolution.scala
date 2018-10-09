/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.solutions.datastream_scala.windows

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Scala reference implementation for the "Popular Places" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The task of the exercise is to identify every five minutes popular areas where many taxi rides
  * arrived or departed in the last 15 minutes.
  *
  * Parameters:
  * -input path-to-input-file
  * 寻找热门地点
  *
  * 返回的数据流：
  * Tuple5<Float, Float, Long, Boolean, Integer> 位置单元格的经度，纬度，计数的时间戳，到达或离开计数的标志（布尔值），实际计数
  */
object PopularPlacesSolution {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", pathToRideData)
    val popThreshold = params.getInt("threshold", 20)

    val maxDelay = 60 // events are out of order by max 60 seconds
    val speed = 600   // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    // find n most popular spots
    val popularPlaces = rides
      // 排除所有不在NYC的数据
      .filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }
      // 转换为  (gridId, true)  gridId 代表一个100x100米大小的区域的单元ID，true 代表是开始事件
      .map(new GridCellMatcher)
      // 根据区域单元id和事件类型分组
      .keyBy(k => k)
      // 构建滑动窗口
      .timeWindow(Time.minutes(15), Time.minutes(5))
      // 对窗口内的事件计数
      .apply { (key: (Int, Boolean), window, vals, out: Collector[(Int, Long, Boolean, Int)]) =>

      //（gridId, (window 结束时的时间戳)，开始/结束 事件，当前分组之后组内的数量）
      out.collect((key._1, window.getEnd, key._2, vals.size))
    }
      // 根据阈值过滤，
      .filter(c => { c._4 >= popThreshold })
      // 把区域坐标id 转换回 经度 和 纬度
      // map之后的数据   （经度，纬度, (window 的结束)，开始/结束 事件，当前分组之后组内的数量）
      .map(new GridToCoordinates)

    // print result on stdout
    printOrTest(popularPlaces)

    // execute the transformation pipeline
    env.execute("Popular Places")
  }

  /**
    * Map taxi ride to grid cell and event type.
    * Start records use departure location, end record use arrival location.
    */
  class GridCellMatcher extends MapFunction[TaxiRide, (Int, Boolean)] {

    def map(taxiRide: TaxiRide): (Int, Boolean) = {
      if (taxiRide.isStart) {
        // 根据经度和纬度 来生成 一个大约100x100米大小的区域的单元ID。
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat)
        (gridId, true) // true 代表 开始地点
      } else {
        //
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat)
        (gridId, false)
      }
    }
  }

  /**
    * 把区域坐标id 转换回 经度 和 纬度
    */
  class GridToCoordinates extends MapFunction[
    (Int, Long, Boolean, Int),
    (Float, Float, Long, Boolean, Int)] {

    def map(cellCount: (Int, Long, Boolean, Int)): (Float, Float, Long, Boolean, Int) = {
      val longitude = GeoUtils.getGridCellCenterLon(cellCount._1)
      val latitude = GeoUtils.getGridCellCenterLat(cellCount._1)
      (longitude, latitude, cellCount._2, cellCount._3, cellCount._4)
    }
  }

}

