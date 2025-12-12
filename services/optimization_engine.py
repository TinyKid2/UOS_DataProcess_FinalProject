from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from datetime import datetime
import numpy as np
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PowerControlStrategy:
    """전력 제어 전략 데이터 클래스"""
    battery_charge_rate: float  # 배터리 충전 속도 (kW)
    battery_discharge_rate: float  # 배터리 방전 속도 (kW)
    grid_import: float  # 그리드에서 가져올 전력 (kW)
    grid_export: float  # 그리드로 보낼 전력 (kW)
    load_shedding: float  # 부하 차단 (kW)
    renewable_curtailment: float  # 재생에너지 출력 제한 (kW)
    estimated_cost: float  # 예상 비용
    efficiency_score: float  # 효율성 점수

class MicrogridOptimizationEngine:
    def __init__(self):
        self.kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.input_topic = 'control_optimization_request'
        self.output_topic = 'control_result'
        
        # 마이크로그리드 파라미터
        self.battery_capacity = 5000  # kWh
        self.battery_soc = 0.5  # State of Charge (0-1)
        self.battery_max_charge = 1000  # kW
        self.battery_max_discharge = 1000  # kW
        self.battery_efficiency = 0.95
        
        # 전력 가격 (시간대별)
        self.electricity_prices = self._init_electricity_prices()
        
        # 부하 프로필 (시간대별 평균)
        self.load_profiles = self._init_load_profiles()
        
        # Kafka Consumer 설정
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.kafka_bootstrap,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='optimization_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Kafka Producer 설정
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        logger.info(f"Microgrid Optimization Engine initialized - Bootstrap: {self.kafka_bootstrap}")
    
    def _init_electricity_prices(self) -> Dict[int, float]:
        """시간대별 전력 가격 초기화 (원/kWh)"""
        prices = {}
        for hour in range(24):
            if 9 <= hour <= 11 or 17 <= hour <= 21:  # 피크 시간
                prices[hour] = 150
            elif 7 <= hour <= 9 or 11 <= hour <= 17:  # 중간 부하
                prices[hour] = 100
            else:  # 경부하
                prices[hour] = 60
        return prices
    
    def _init_load_profiles(self) -> Dict[str, Dict[int, float]]:
        """마이크로그리드별 부하 프로필 초기화 (kW)"""
        profiles = {
            'MicroGrid-01': {},  # 서울 - 상업 지역
            'MicroGrid-02': {},  # 대전 - 산업 지역
            'MicroGrid-03': {}   # 부산 - 주거 지역
        }
        
        # 각 지역별 특성에 맞는 부하 프로필 생성
        for hour in range(24):
            # 상업 지역: 낮 시간 피크
            if 9 <= hour <= 18:
                profiles['MicroGrid-01'][hour] = 2000 + np.random.normal(0, 100)
            else:
                profiles['MicroGrid-01'][hour] = 800 + np.random.normal(0, 50)
            
            # 산업 지역: 24시간 일정
            profiles['MicroGrid-02'][hour] = 3000 + np.random.normal(0, 200)
            
            # 주거 지역: 아침, 저녁 피크
            if 7 <= hour <= 9 or 18 <= hour <= 22:
                profiles['MicroGrid-03'][hour] = 1500 + np.random.normal(0, 100)
            elif 10 <= hour <= 17:
                profiles['MicroGrid-03'][hour] = 600 + np.random.normal(0, 50)
            else:
                profiles['MicroGrid-03'][hour] = 400 + np.random.normal(0, 30)
        
        return profiles
    
    def estimate_load(self, location: str, hour: int, is_weekend: bool) -> float:
        """현재 부하 추정"""
        base_load = self.load_profiles.get(location, {}).get(hour, 1000)
        
        # 주말 보정
        if is_weekend:
            if 'MicroGrid-01' in location:  # 상업 지역
                base_load *= 0.3
            elif 'MicroGrid-02' in location:  # 산업 지역
                base_load *= 0.5
            elif 'MicroGrid-03' in location:  # 주거 지역
                base_load *= 1.2
        
        return max(0, base_load)
    
    def calculate_power_balance(self, request: dict) -> Tuple[float, float]:
        """전력 균형 계산"""
        power_data = request['power_data']
        temporal = request['temporal_features']
        
        # 발전량
        total_generation = power_data['total']
        
        # 예상 부하
        estimated_load = self.estimate_load(
            request['location'],
            temporal['hour'],
            temporal['is_weekend']
        )
        
        # 전력 균형
        power_balance = total_generation - estimated_load
        
        return power_balance, estimated_load
    
    def optimize_battery_operation(self, power_balance: float, hour: int) -> Tuple[float, float]:
        """배터리 운영 최적화"""
        electricity_price = self.electricity_prices[hour]
        
        charge_rate = 0
        discharge_rate = 0
        
        if power_balance > 0:  # 잉여 전력
            # 배터리 충전 (가격이 낮거나 SOC가 낮을 때)
            if self.battery_soc < 0.9 and electricity_price < 100:
                charge_rate = min(
                    power_balance,
                    self.battery_max_charge,
                    (self.battery_capacity * (1 - self.battery_soc))
                )
                self.battery_soc += (charge_rate * self.battery_efficiency) / self.battery_capacity
        else:  # 전력 부족
            # 배터리 방전 (가격이 높거나 긴급 시)
            if self.battery_soc > 0.2 and (electricity_price > 100 or abs(power_balance) > 500):
                discharge_rate = min(
                    abs(power_balance),
                    self.battery_max_discharge,
                    self.battery_capacity * self.battery_soc
                )
                self.battery_soc -= discharge_rate / (self.battery_capacity * self.battery_efficiency)
        
        return charge_rate, discharge_rate
    
    def optimize_grid_interaction(self, power_balance: float, hour: int, 
                                 battery_charge: float, battery_discharge: float) -> Tuple[float, float]:
        """그리드 상호작용 최적화"""
        electricity_price = self.electricity_prices[hour]
        
        # 배터리 작동 후 순 전력
        net_power = power_balance - battery_charge + battery_discharge
        
        grid_import = 0
        grid_export = 0
        
        if net_power > 0:  # 여전히 잉여 전력
            # 가격이 높을 때 판매
            if electricity_price > 80:
                grid_export = net_power
        else:  # 여전히 전력 부족
            # 그리드에서 구매
            grid_import = abs(net_power)
        
        return grid_import, grid_export
    
    def calculate_control_strategy(self, request: dict) -> PowerControlStrategy:
        """전력 제어 전략 계산"""
        # 전력 균형 계산
        power_balance, estimated_load = self.calculate_power_balance(request)
        
        temporal = request['temporal_features']
        hour = temporal['hour']
        
        # 배터리 운영 최적화
        battery_charge, battery_discharge = self.optimize_battery_operation(power_balance, hour)
        
        # 그리드 상호작용 최적화
        grid_import, grid_export = self.optimize_grid_interaction(
            power_balance, hour, battery_charge, battery_discharge
        )
        
        # 부하 차단 계산 (필요시)
        load_shedding = 0
        if grid_import > 2000:  # 임계값 초과
            load_shedding = min(grid_import - 2000, estimated_load * 0.2)
            grid_import -= load_shedding
        
        # 재생에너지 출력 제한 (필요시)
        renewable_curtailment = 0
        if grid_export > 3000:  # 그리드 용량 제한
            renewable_curtailment = grid_export - 3000
            grid_export = 3000
        
        # 비용 계산
        electricity_price = self.electricity_prices[hour]
        cost = (grid_import * electricity_price) - (grid_export * electricity_price * 0.9)
        
        # 효율성 점수 계산
        total_generation = request['power_data']['total']
        utilized_power = total_generation - renewable_curtailment
        efficiency_score = (utilized_power / max(total_generation, 1)) * (1 - load_shedding / max(estimated_load, 1))
        
        return PowerControlStrategy(
            battery_charge_rate=battery_charge,
            battery_discharge_rate=battery_discharge,
            grid_import=grid_import,
            grid_export=grid_export,
            load_shedding=load_shedding,
            renewable_curtailment=renewable_curtailment,
            estimated_cost=cost,
            efficiency_score=efficiency_score
        )
    
    def prepare_control_result(self, request: dict, strategy: PowerControlStrategy) -> dict:
        """제어 결과 메시지 준비"""
        result = {
            'result_id': f"ctrl_{request['request_id']}",
            'request_id': request['request_id'],
            'timestamp': datetime.now().isoformat(),
            'location': request['location'],
            'power_data': request['power_data'],
            'control_strategy': {
                'battery': {
                    'charge_rate': strategy.battery_charge_rate,
                    'discharge_rate': strategy.battery_discharge_rate,
                    'current_soc': self.battery_soc * 100,  # 백분율
                    'capacity': self.battery_capacity
                },
                'grid': {
                    'import': strategy.grid_import,
                    'export': strategy.grid_export
                },
                'demand_response': {
                    'load_shedding': strategy.load_shedding,
                    'renewable_curtailment': strategy.renewable_curtailment
                }
            },
            'metrics': {
                'estimated_cost': strategy.estimated_cost,
                'efficiency_score': strategy.efficiency_score,
                'renewable_utilization': 1 - (strategy.renewable_curtailment / max(request['power_data']['total'], 1)),
                'grid_dependency': strategy.grid_import / max(strategy.grid_import + request['power_data']['total'], 1)
            },
            'optimization_params': {
                'electricity_price': self.electricity_prices[request['temporal_features']['hour']],
                'quality_score': request.get('quality_score', 1.0)
            },
            'original_timestamp': request['original_timestamp']
        }
        
        return result
    
    def process_optimization_request(self, request: dict) -> bool:
        """최적화 요청 처리"""
        try:
            logger.info(f"Processing optimization request for {request['location']}")
            
            # 제어 전략 계산
            strategy = self.calculate_control_strategy(request)
            
            # 결과 메시지 준비
            result = self.prepare_control_result(request, strategy)
            
            # Kafka로 전송
            future = self.producer.send(self.output_topic, value=result)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Optimization complete - Location: {request['location']}, "
                       f"Efficiency: {strategy.efficiency_score:.2f}, "
                       f"Cost: {strategy.estimated_cost:.0f} KRW")
            
            # 상세 로그
            logger.debug(f"Control Strategy: Battery Charge={strategy.battery_charge_rate:.1f}kW, "
                        f"Discharge={strategy.battery_discharge_rate:.1f}kW, "
                        f"Grid Import={strategy.grid_import:.1f}kW, "
                        f"Export={strategy.grid_export:.1f}kW")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing optimization request: {e}")
            return False
    
    def run(self):
        """서비스 실행"""
        logger.info("Starting Microgrid Optimization Engine...")
        
        try:
            for message in self.consumer:
                request = message.value
                logger.info(f"Received optimization request from topic '{self.input_topic}'")
                
                # 최적화 요청 처리
                success = self.process_optimization_request(request)
                
                if not success:
                    logger.error(f"Failed to process optimization request: {request.get('request_id')}")
                
        except KeyboardInterrupt:
            logger.info("Shutting down Optimization Engine...")
        except Exception as e:
            logger.error(f"Service error: {e}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    # Kafka가 준비될 때까지 대기
    time.sleep(15)
    
    engine = MicrogridOptimizationEngine()
    engine.run()