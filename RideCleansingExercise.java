/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */
public class RideCleansingExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// Чтение параметров командной строки
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // максимальная задержка событий — 60 секунд
		final int servingSpeedFactor = 600; // 10 минут данных подаются за 1 секунду реального времени

		// Настройка среды выполнения потоков
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// Создание потока данных о поездках
		DataStream<TaxiRide> rides = env.addSource(
				rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor))
		);

		// Фильтрация поездок: оставляем только те, что начинаются и заканчиваются в пределах Нью-Йорка
		DataStream<TaxiRide> filteredRides = rides
				.filter(new NYCFilter());

		// Вывод отфильтрованного потока
		printOrTest(filteredRides);

		// Запуск выполнения пайплайна
		env.execute("Taxi Ride Cleansing");
	}

	/**
	 * Фильтр для отбора поездок, которые полностью проходят в пределах Нью-Йорка.
	 */
	private static class NYCFilter implements FilterFunction<TaxiRide> {

		// Географические границы Нью-Йорка
		private static final float MIN_LATITUDE = 40.4774f;
		private static final float MAX_LATITUDE = 40.9176f;
		private static final float MIN_LONGITUDE = -74.2591f;
		private static final float MAX_LONGITUDE = -73.7004f;

		/**
		 * Фильтрация поездки: возвращает true, если поездка начинается и заканчивается в пределах Нью-Йорка.
		 *
		 * @param taxiRide поездка на такси
		 * @return true, если поездка внутри NYC, иначе false
		 */
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return isWithinNYC(taxiRide.startLat, taxiRide.startLon)
					&& isWithinNYC(taxiRide.endLat, taxiRide.endLon);
		}

		/**
		 * Проверяет, находится ли заданная точка внутри границ Нью-Йорка.
		 *
		 * @param latitude широта
		 * @param longitude долгота
		 * @return true, если точка в пределах NYC, иначе false
		 */
		private boolean isWithinNYC(float latitude, float longitude) {
			return latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE
					&& longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
		}
	}
}