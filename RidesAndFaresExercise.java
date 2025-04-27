/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// Extract parameters from the input arguments
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesInput = params.get("rides", pathToRideData);
		String faresInput = params.get("fares", pathToFareData);

		// Configuration parameters
		int maxDelay = 60;
		int speedFactor = 1800;

		// Setup the execution environment with checkpoints and web UI
		Configuration configuration = new Configuration();
		configuration.setString("state.backend", "filesystem");
		configuration.setString("state.savepoints.dir", "file:///tmp/savepoints");
		configuration.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(ExerciseBase.parallelism);

		env.enableCheckpointing(10000L);
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<TaxiRide> rideStream = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesInput, maxDelay, speedFactor)))
				.filter(ride -> ride.isStart)
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fareStream = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresInput, maxDelay, speedFactor)))
				.keyBy(fare -> fare.rideId);

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedStream = rideStream
				.connect(fareStream)
				.flatMap(new RideFareEnrichmentFunction())
				.uid("ride-fare-enrichment");

		printOrTest(enrichedStream);

		env.execute("Enrich Taxi Rides with Fares");
	}

	/**
	 * Функция CoFlatMap для объединения событий TaxiRide и TaxiFare по rideId.
	 *
	 * В зависимости от выполнения событий происходит следующее:
	 * - Если приходят оба события (поездка и оплата), они объединяются и эмитятся как Tuple2.
	 * - Если приходит только одно событие, оно сохраняется в состоянии.
	 * - Таймеры не используются: ожидается, что соответствующее событие когда-нибудь придёт.
	 */
	public static class RideFareEnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		/**
		 * Инициализация ValueState для хранения несопоставленных поездок и оплат.
		 */
		@Override
		public void open(Configuration parameters) {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("ride-state", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("fare-state", TaxiFare.class));
		}

		/**
		 * Обработка события TaxiRide.
		 * Если соответствующее событие TaxiFare уже сохранено, объединяет их и эмитит.
		 * Если оплаты нет, сохраняет поездку в состоянии.
		 *
		 * @param ride событие поездки
		 * @param collector коллектор для вывода объединённых событий
		 */
		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
			TaxiFare fare = fareState.value();

			if (fare == null) {
				rideState.update(ride);
				return;
			}

			fareState.clear();
			collector.collect(Tuple2.of(ride, fare));
		}

		/**
		 * Обработка события TaxiFare.
		 * Если соответствующее событие TaxiRide уже сохранено, объединяет их и эмитит.
		 * Если поездки нет, сохраняет оплату в состоянии.
		 *
		 * @param fare событие оплаты
		 * @param collector коллектор для вывода объединённых событий
		 */
		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> collector) throws Exception {
			TaxiRide ride = rideState.value();

			if (ride == null) {
				fareState.update(fare);
				return;
			}

			rideState.clear();
			collector.collect(Tuple2.of(ride, fare));
		}
	}
}