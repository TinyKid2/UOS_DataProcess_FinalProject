from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import logging
import os
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SolarDataSparkProcessor:
    def __init__(self, app_name: str = "SolarPowerETL"):
        """
        Spark-based processor for solar power data
        Based on textbook Chapter 11 - Big Data Processing with Spark
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.data_path = os.path.join(os.path.dirname(__file__), "..", "data")
        os.makedirs(self.data_path, exist_ok=True)
        
        logger.info(f"SparkSession 초기화 완료 - App: {app_name}")
    
    def create_schema(self):
        """데이터 스키마를 정의합니다"""
        return StructType([
            StructField("거래일자", StringType(), True),
            StructField("거래시간", IntegerType(), True),
            StructField("지역", StringType(), True),
            StructField("태양광 발전량(MWh)", DoubleType(), True)
        ])
    
    def load_data_from_api(self, api_url: str, service_key: str, pages: int = 5) -> DataFrame:
        """API 데이터를 Spark DataFrame으로 로드합니다"""
        import requests
        
        all_data = []
        
        for page in range(1, pages + 1):
            try:
                params = {
                    'page': page,
                    'perPage': 100,
                    'serviceKey': service_key
                }
                
                response = requests.get(api_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        all_data.extend(data['data'])
                        logger.info(f"페이지 {page} 로드 완료")
                        
            except Exception as e:
                logger.error(f"데이터 로드 실패: {str(e)}")
        
        if all_data:
            schema = self.create_schema()
            df = self.spark.createDataFrame(all_data, schema)
            logger.info(f"총 {df.count()} 건의 데이터를 Spark DataFrame으로 변환")
            return df
        else:
            return self.spark.createDataFrame([], self.create_schema())
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """데이터 변환 및 특성 엔지니어링"""
        logger.info("데이터 변환 시작")
        
        df = df.withColumnRenamed("거래일자", "transaction_date") \
               .withColumnRenamed("거래시간", "transaction_hour") \
               .withColumnRenamed("지역", "region") \
               .withColumnRenamed("태양광 발전량(MWh)", "solar_generation_mwh")
        
        df = df.withColumn("transaction_date", to_date(col("transaction_date")))
        
        df = df.withColumn("year", year("transaction_date")) \
               .withColumn("month", month("transaction_date")) \
               .withColumn("day", dayofmonth("transaction_date")) \
               .withColumn("weekday", dayofweek("transaction_date")) \
               .withColumn("quarter", quarter("transaction_date"))
        
        df = df.withColumn("is_weekend", 
                          when(col("weekday").isin([1, 7]), 1).otherwise(0))
        
        df = df.withColumn("is_daylight",
                          when((col("transaction_hour") >= 6) & 
                               (col("transaction_hour") <= 18), 1).otherwise(0))
        
        df = df.withColumn("hour_category",
                          when(col("transaction_hour") < 6, "dawn")
                          .when(col("transaction_hour") < 12, "morning")
                          .when(col("transaction_hour") < 18, "afternoon")
                          .otherwise("night"))
        
        df = df.na.fill({"solar_generation_mwh": 0})
        
        windowSpec = Window.partitionBy("region").orderBy("transaction_date", "transaction_hour")
        df = df.withColumn("prev_generation", lag("solar_generation_mwh", 1).over(windowSpec))
        df = df.withColumn("generation_diff", 
                          col("solar_generation_mwh") - col("prev_generation"))
        
        daily_window = Window.partitionBy("region", "transaction_date")
        df = df.withColumn("daily_total", sum("solar_generation_mwh").over(daily_window)) \
               .withColumn("daily_avg", avg("solar_generation_mwh").over(daily_window)) \
               .withColumn("daily_max", max("solar_generation_mwh").over(daily_window))
        
        logger.info(f"데이터 변환 완료 - {df.count()} 레코드")
        
        return df
    
    def aggregate_statistics(self, df: DataFrame):
        """통계 집계를 수행합니다"""
        logger.info("통계 집계 시작")
        
        overall_stats = df.agg(
            count("*").alias("total_records"),
            avg("solar_generation_mwh").alias("avg_generation"),
            max("solar_generation_mwh").alias("max_generation"),
            stddev("solar_generation_mwh").alias("std_generation"),
            sum("solar_generation_mwh").alias("total_generation")
        ).collect()[0]
        
        logger.info(f"전체 통계 - 레코드: {overall_stats['total_records']}, "
                   f"평균: {overall_stats['avg_generation']:.2f} MWh")
        
        regional_stats = df.groupBy("region").agg(
            count("*").alias("count"),
            avg("solar_generation_mwh").alias("avg_generation"),
            max("solar_generation_mwh").alias("max_generation"),
            sum("solar_generation_mwh").alias("total_generation")
        ).orderBy(desc("avg_generation"))
        
        print("\n지역별 통계 (Top 10):")
        regional_stats.show(10)
        
        hourly_pattern = df.groupBy("transaction_hour").agg(
            avg("solar_generation_mwh").alias("avg_generation")
        ).orderBy("transaction_hour")
        
        print("\n시간대별 평균 발전량:")
        hourly_pattern.show(24)
        
        return {
            "overall": overall_stats.asDict(),
            "regional": regional_stats.collect(),
            "hourly": hourly_pattern.collect()
        }
    
    def train_ml_model(self, df: DataFrame):
        """머신러닝 모델을 학습합니다"""
        logger.info("ML 모델 학습 시작")
        
        df_ml = df.filter(col("solar_generation_mwh") > 0)
        
        feature_cols = ["transaction_hour", "month", "day", "weekday", 
                       "is_weekend", "is_daylight"]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
        
        lr = LinearRegression(
            featuresCol="features",
            labelCol="solar_generation_mwh",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.8
        )
        
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="solar_generation_mwh",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline_lr = Pipeline(stages=[assembler, lr])
        pipeline_rf = Pipeline(stages=[assembler, rf])
        
        logger.info("Linear Regression 모델 학습 중...")
        model_lr = pipeline_lr.fit(train_data)
        
        logger.info("Random Forest 모델 학습 중...")
        model_rf = pipeline_rf.fit(train_data)
        
        evaluator = RegressionEvaluator(
            labelCol="solar_generation_mwh",
            predictionCol="prediction"
        )
        
        pred_lr = model_lr.transform(test_data)
        pred_rf = model_rf.transform(test_data)
        
        rmse_lr = evaluator.evaluate(pred_lr, {evaluator.metricName: "rmse"})
        r2_lr = evaluator.evaluate(pred_lr, {evaluator.metricName: "r2"})
        
        rmse_rf = evaluator.evaluate(pred_rf, {evaluator.metricName: "rmse"})
        r2_rf = evaluator.evaluate(pred_rf, {evaluator.metricName: "r2"})
        
        logger.info(f"Linear Regression - RMSE: {rmse_lr:.4f}, R2: {r2_lr:.4f}")
        logger.info(f"Random Forest - RMSE: {rmse_rf:.4f}, R2: {r2_rf:.4f}")
        
        model_path = os.path.join(self.data_path, "spark_ml_models")
        model_lr.write().overwrite().save(os.path.join(model_path, "linear_regression"))
        model_rf.write().overwrite().save(os.path.join(model_path, "random_forest"))
        
        return {
            "linear_regression": {"rmse": rmse_lr, "r2": r2_lr},
            "random_forest": {"rmse": rmse_rf, "r2": r2_rf}
        }
    
    def stream_processing(self, kafka_servers: str = "localhost:9092", 
                         topic: str = "solar_raw_data"):
        """실시간 스트림 처리"""
        logger.info(f"스트림 처리 시작 - Topic: {topic}")
        
        df_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        schema = StructType([
            StructField("timestamp", StringType()),
            StructField("data_source", StringType()),
            StructField("record", StructType([
                StructField("거래일자", StringType()),
                StructField("거래시간", IntegerType()),
                StructField("지역", StringType()),
                StructField("태양광 발전량(MWh)", DoubleType())
            ]))
        ])
        
        df_parsed = df_stream.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.record.*")
        
        df_windowed = df_parsed \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("지역")
            ).agg(
                avg("태양광 발전량(MWh)").alias("avg_generation"),
                max("태양광 발전량(MWh)").alias("max_generation"),
                count("*").alias("count")
            )
        
        query = df_windowed.writeStream \
            .outputMode("update") \
            .format("console") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def save_to_parquet(self, df: DataFrame, output_name: str):
        """데이터를 Parquet 형식으로 저장합니다"""
        output_path = os.path.join(self.data_path, f"{output_name}.parquet")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("region", "year", "month") \
            .parquet(output_path)
        
        logger.info(f"데이터 저장 완료: {output_path}")
    
    def load_from_parquet(self, file_name: str) -> DataFrame:
        """Parquet 파일을 로드합니다"""
        file_path = os.path.join(self.data_path, f"{file_name}.parquet")
        
        df = self.spark.read.parquet(file_path)
        logger.info(f"데이터 로드 완료: {file_path} ({df.count()} 레코드)")
        
        return df
    
    def generate_spark_report(self, stats: dict) -> str:
        """Spark 처리 결과 리포트를 생성합니다"""
        report = []
        report.append("=" * 60)
        report.append("Spark 데이터 처리 리포트")
        report.append("=" * 60)
        report.append(f"생성 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        if "overall" in stats:
            overall = stats["overall"]
            report.append("1. 전체 통계")
            report.append("-" * 40)
            report.append(f"  총 레코드: {overall['total_records']:,}")
            report.append(f"  평균 발전량: {overall['avg_generation']:.2f} MWh")
            report.append(f"  최대 발전량: {overall['max_generation']:.2f} MWh")
            report.append(f"  총 발전량: {overall['total_generation']:.2f} MWh")
            report.append("")
        
        if "regional" in stats and stats["regional"]:
            report.append("2. 지역별 Top 5")
            report.append("-" * 40)
            for i, region in enumerate(stats["regional"][:5], 1):
                report.append(f"  {i}. {region['region']}: "
                            f"평균 {region['avg_generation']:.2f} MWh, "
                            f"총 {region['total_generation']:.2f} MWh")
            report.append("")
        
        report.append("=" * 60)
        
        return "\n".join(report)
    
    def close(self):
        """SparkSession을 종료합니다"""
        self.spark.stop()
        logger.info("SparkSession 종료")


def main():
    """메인 실행 함수"""
    print("\n태양광 발전량 Spark 처리 시스템")
    print("=" * 50)
    print("1. 배치 처리 및 분석")
    print("2. ML 모델 학습")
    print("3. 스트림 처리 시작")
    print("4. Parquet 파일 저장")
    print("5. Parquet 파일 로드 및 분석")
    print("6. 종료")
    print("=" * 50)
    
    processor = SolarDataSparkProcessor()
    df = None
    
    api_url = "https://api.odcloud.kr/api/15065269/v1/uddi:166a7ee3-161a-4dc2-9f59-cedb6b1c7213"
    service_key = "9562cdd6a0ca84905cca10836104720138ae6edf962921c1d29c67f6e72fc8db"
    
    try:
        while True:
            choice = input("\n선택하세요 (1-6): ").strip()
            
            if choice == '1':
                pages = int(input("로드할 페이지 수 (기본값: 5): ") or "5")
                df = processor.load_data_from_api(api_url, service_key, pages)
                df = processor.transform_data(df)
                stats = processor.aggregate_statistics(df)
                
                report = processor.generate_spark_report(stats)
                print("\n" + report)
                
            elif choice == '2':
                if df is None:
                    print("먼저 데이터를 로드해주세요 (옵션 1)")
                else:
                    results = processor.train_ml_model(df)
                    print("\n모델 학습 결과:")
                    for model_name, metrics in results.items():
                        print(f"{model_name}: RMSE={metrics['rmse']:.4f}, R2={metrics['r2']:.4f}")
                
            elif choice == '3':
                kafka_servers = input("Kafka 서버 (기본값: localhost:9092): ") or "localhost:9092"
                topic = input("토픽 이름 (기본값: solar_raw_data): ") or "solar_raw_data"
                
                print("스트림 처리를 시작합니다. 중단하려면 Ctrl+C를 누르세요.")
                query = processor.stream_processing(kafka_servers, topic)
                query.awaitTermination()
                
            elif choice == '4':
                if df is None:
                    print("먼저 데이터를 로드해주세요 (옵션 1)")
                else:
                    output_name = input("저장할 파일명 (기본값: solar_data): ") or "solar_data"
                    processor.save_to_parquet(df, output_name)
                    print("Parquet 파일 저장 완료")
                
            elif choice == '5':
                file_name = input("로드할 Parquet 파일명 (기본값: solar_data): ") or "solar_data"
                df = processor.load_from_parquet(file_name)
                df.show(20)
                
            elif choice == '6':
                print("프로그램을 종료합니다.")
                break
                
            else:
                print("올바른 선택지를 입력해주세요 (1-6)")
                
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류: {str(e)}")
    finally:
        processor.close()


if __name__ == "__main__":
    main()