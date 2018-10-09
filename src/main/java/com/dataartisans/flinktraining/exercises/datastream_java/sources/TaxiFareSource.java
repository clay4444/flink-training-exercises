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

package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of TaxiFare records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * 为了尽可能逼真地生成流，事件与其时间戳成比例地发出。实际上相隔十分钟后发生的两件事也在十分钟之后发生。
 *
 * 可以指定加速因子来“快进”流，即，给定加速因子60，在一分钟内发生的事件在一秒内被服务。
 *
 * 此外，可以指定最大服务延迟，这导致每个事件在指定范围内随机延迟。
 *
 * 这产生了无序流，这在许多实际应用中是常见的。
 *
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it is able to operate in event time mode which is configured as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 */
public class TaxiFareSource implements SourceFunction<TaxiFare> {

	private final int maxDelayMsecs;        // 指定最大服务延迟，这导致每个事件在指定范围内随机延迟。
	private final int watermarkDelayMSecs;  // watermark 等待时间

	private final String dataFilePath;      //文件路径
	private final int servingSpeed;  //加速因子

	private transient BufferedReader reader;
	private transient InputStream gzipStream;

	/**
	 * Serves the TaxiFare records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiFare records are read.
	 */
	public TaxiFareSource(String dataFilePath) {
		this(dataFilePath, 0, 1);
	}

	/**
	 * Serves the TaxiFare records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiFare records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiFareSource(String dataFilePath, int servingSpeedFactor) {
		this(dataFilePath, 0, servingSpeedFactor);
	}

	/**
	 * Serves the TaxiFare records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiFare records are read.
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiFareSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
		if(maxEventDelaySecs < 0) {
			throw new IllegalArgumentException("Max event delay must be positive");
		}
		this.dataFilePath = dataFilePath;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;  //watermark 等待时间 = 最大服务延迟
		this.servingSpeed = servingSpeedFactor;
	}

	@Override
	public void run(SourceContext<TaxiFare> sourceContext) throws Exception {

		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

		generateUnorderedStream(sourceContext);

		this.reader.close();
		this.reader = null;
		this.gzipStream.close();
		this.gzipStream = null;

	}

	//一直生成数据流的方法
	private void generateUnorderedStream(SourceContext<TaxiFare> sourceContext) throws Exception {

	    // 当前时间   毫秒数
		long servingStartTime = Calendar.getInstance().getTimeInMillis();
		long dataStartTime;

		Random rand = new Random(7452);

		// 优先级队列，Long 是事件的开始时间戳+随机延迟时间，目的是模拟乱序数据
		PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
				32,
				new Comparator<Tuple2<Long, Object>>() {
					@Override
					public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
						return o1.f0.compareTo(o2.f0);
					}
				});

		// read first ride and insert it into emit schedule
		String line;
		TaxiFare fare;
		if (reader.ready() && (line = reader.readLine()) != null) {
			// 读取第一个TaxiFare 数据对象
			fare = TaxiFare.fromString(line);
			// 抽取开始时间戳
			dataStartTime = getEventTime(fare);   //获取当前数据的开始时间，毫秒
			// 获取延迟时间
			long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

			emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, fare));
			// schedule next watermark
			long watermarkTime = dataStartTime + watermarkDelayMSecs;
			Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
			emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

		} else {
			return;
		}

		// peek at next ride
		if (reader.ready() && (line = reader.readLine()) != null) {
			fare = TaxiFare.fromString(line);
		}

		// read rides one-by-one and emit a random ride from the buffer each time
		while (emitSchedule.size() > 0 || reader.ready()) {

			// insert all events into schedule that might be emitted next
			long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
			long rideEventTime = fare != null ? getEventTime(fare) : -1;
			while(
					fare != null && ( // while there is a ride AND
						emitSchedule.isEmpty() || // and no ride in schedule OR
						rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
					)
			{
				// insert event into emit schedule
				long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
				emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, fare));

				// read next ride
				if (reader.ready() && (line = reader.readLine()) != null) {
					fare = TaxiFare.fromString(line);
					rideEventTime = getEventTime(fare);
				}
				else {
					fare = null;
					rideEventTime = -1;
				}
			}

			// emit schedule is updated, emit next element in schedule
			Tuple2<Long, Object> head = emitSchedule.poll();
			long delayedEventTime = head.f0;

			long now = Calendar.getInstance().getTimeInMillis();
			long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
			long waitTime = servingTime - now;

			Thread.sleep( (waitTime > 0) ? waitTime : 0);

			if(head.f1 instanceof TaxiFare) {
				TaxiFare emitFare = (TaxiFare)head.f1;
				// emit ride
				sourceContext.collectWithTimestamp(emitFare, getEventTime(emitFare));
			}
			else if(head.f1 instanceof Watermark) {
				Watermark emitWatermark = (Watermark)head.f1;
				// emit watermark
				sourceContext.emitWatermark(emitWatermark);
				// schedule next watermark
				long watermarkTime = delayedEventTime + watermarkDelayMSecs;
				Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
				emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
			}
		}
	}

	public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
		long dataDiff = eventTime - dataStartTime;
		return servingStartTime + (dataDiff / this.servingSpeed);
	}

	public long getEventTime(TaxiFare fare) {
		return fare.getEventTime();
	}

	public long getNormalDelayMsecs(Random rand) {
		long delay = -1;
		long x = maxDelayMsecs / 2;
		while(delay < 0 || delay > maxDelayMsecs) {
			delay = (long)(rand.nextGaussian() * x) + x;
		}
		return delay;
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}

}

