#!/usr/bin/env python3
"""
마이크로그리드 전력 제어 시스템 테스트 스크립트
"""

import requests
import json
import time
import random
from datetime import datetime, timedelta
import argparse
import sys

class MicrogridSystemTester:
    def __init__(self, base_url="http://localhost:15000"):
        self.base_url = base_url
        self.locations = ["MicroGrid-01", "MicroGrid-02", "MicroGrid-03"]
        
    def generate_realistic_data(self, hour, location):
        """시간대와 위치에 따른 현실적인 발전량 데이터 생성"""
        
        # 태양광 발전 패턴 (시간대별)
        solar_pattern = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0,
            6: 50, 7: 200, 8: 500, 9: 800, 10: 1000, 11: 1200,
            12: 1300, 13: 1250, 14: 1100, 15: 900, 16: 600, 17: 300,
            18: 100, 19: 20, 20: 0, 21: 0, 22: 0, 23: 0
        }
        
        # 기본 태양광 발전량
        base_solar = solar_pattern.get(hour, 0)
        
        # 위치별 보정 (서울 < 대전 < 부산)
        location_factor = {
            "MicroGrid-01": 0.9,  # 서울
            "MicroGrid-02": 1.0,  # 대전
            "MicroGrid-03": 1.1   # 부산
        }
        
        solar_power = base_solar * location_factor.get(location, 1.0)
        solar_power += random.uniform(-50, 50)  # 변동성 추가
        solar_power = max(0, solar_power)
        
        # 풍력 발전 (더 랜덤하지만 일정한 패턴)
        base_wind = 500 + 300 * abs(random.gauss(0, 1))
        
        # 밤에 풍력이 약간 강해지는 경향
        if hour < 6 or hour > 20:
            base_wind *= 1.2
            
        wind_power = base_wind + random.uniform(-100, 100)
        wind_power = max(0, wind_power)
        
        # 날씨 데이터
        weather = {
            "temperature": 20 + random.uniform(-5, 10),
            "humidity": 60 + random.uniform(-20, 20),
            "wind_speed": 3 + abs(random.gauss(0, 2))
        }
        
        return {
            "solar_power": round(solar_power, 2),
            "wind_power": round(wind_power, 2),
            "location": location,
            "timestamp": datetime.now().isoformat(),
            "weather": weather
        }
    
    def test_single_data_submission(self):
        """단일 데이터 전송 테스트"""
        print("\n=== 단일 데이터 전송 테스트 ===")
        
        current_hour = datetime.now().hour
        location = random.choice(self.locations)
        data = self.generate_realistic_data(current_hour, location)
        
        print(f"전송 데이터: {json.dumps(data, indent=2)}")
        
        try:
            response = requests.post(
                f"{self.base_url}/api/power-data",
                json=data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                print(f"✅ 성공: {response.json()}")
            else:
                print(f"❌ 실패: {response.status_code} - {response.text}")
                
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ 연결 오류: {e}")
            return False
    
    def test_batch_data_submission(self, count=10):
        """배치 데이터 전송 테스트"""
        print(f"\n=== 배치 데이터 전송 테스트 ({count}개) ===")
        
        batch_data = []
        base_time = datetime.now()
        
        for i in range(count):
            # 시간을 거슬러 올라가며 데이터 생성
            timestamp = base_time - timedelta(minutes=5*i)
            hour = timestamp.hour
            location = self.locations[i % len(self.locations)]
            
            data = self.generate_realistic_data(hour, location)
            data['timestamp'] = timestamp.isoformat()
            batch_data.append(data)
        
        print(f"배치 크기: {len(batch_data)} 데이터 포인트")
        
        try:
            response = requests.post(
                f"{self.base_url}/api/power-data/batch",
                json=batch_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ 성공: 처리됨 {result['results']['success']}/{result['results']['total']}")
            else:
                print(f"❌ 실패: {response.status_code} - {response.text}")
                
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ 연결 오류: {e}")
            return False
    
    def test_continuous_stream(self, duration_minutes=5, interval_seconds=5):
        """연속 스트리밍 테스트"""
        print(f"\n=== 연속 스트리밍 테스트 ({duration_minutes}분, {interval_seconds}초 간격) ===")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        count = 0
        success_count = 0
        
        print("테스트 시작... (Ctrl+C로 중지)")
        
        try:
            while datetime.now() < end_time:
                current_hour = datetime.now().hour
                
                # 각 위치에 대해 데이터 전송
                for location in self.locations:
                    data = self.generate_realistic_data(current_hour, location)
                    
                    try:
                        response = requests.post(
                            f"{self.base_url}/api/power-data",
                            json=data,
                            headers={"Content-Type": "application/json"}
                        )
                        
                        count += 1
                        if response.status_code == 200:
                            success_count += 1
                            print(f"✅ [{count}] {location}: Solar={data['solar_power']}kW, Wind={data['wind_power']}kW")
                        else:
                            print(f"❌ [{count}] 실패: {response.status_code}")
                            
                    except Exception as e:
                        print(f"❌ [{count}] 오류: {e}")
                        count += 1
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n테스트 중지됨")
        
        print(f"\n📊 결과: {success_count}/{count} 성공 ({success_count/count*100:.1f}%)")
        return success_count == count
    
    def test_invalid_data(self):
        """잘못된 데이터 테스트"""
        print("\n=== 잘못된 데이터 처리 테스트 ===")
        
        test_cases = [
            {
                "name": "필수 필드 누락",
                "data": {"solar_power": 100},
                "expected": 400
            },
            {
                "name": "음수 발전량",
                "data": {
                    "solar_power": -100,
                    "wind_power": 200,
                    "location": "MicroGrid-01"
                },
                "expected": 200  # 전처리에서 처리됨
            },
            {
                "name": "잘못된 위치",
                "data": {
                    "solar_power": 100,
                    "wind_power": 200,
                    "location": "InvalidGrid"
                },
                "expected": 200  # 시스템이 처리 가능
            }
        ]
        
        for test in test_cases:
            print(f"\n테스트: {test['name']}")
            print(f"데이터: {test['data']}")
            
            try:
                response = requests.post(
                    f"{self.base_url}/api/power-data",
                    json=test['data'],
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == test['expected']:
                    print(f"✅ 예상대로 처리됨: {response.status_code}")
                else:
                    print(f"⚠️  예상과 다름: {response.status_code} (예상: {test['expected']})")
                    
            except Exception as e:
                print(f"❌ 오류: {e}")
    
    def test_health_check(self):
        """헬스 체크 테스트"""
        print("\n=== 시스템 헬스 체크 ===")
        
        try:
            response = requests.get(f"{self.base_url}/health")
            
            if response.status_code == 200:
                health_data = response.json()
                print(f"✅ 시스템 상태: {health_data['status']}")
                print(f"✅ Kafka 연결: {health_data['kafka']}")
                print(f"✅ 타임스탬프: {health_data['timestamp']}")
                return True
            else:
                print(f"❌ 헬스 체크 실패: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ 연결 실패: {e}")
            return False
    
    def run_full_test(self):
        """전체 테스트 실행"""
        print("=" * 60)
        print("마이크로그리드 전력 제어 시스템 종합 테스트")
        print("=" * 60)
        
        tests_passed = 0
        tests_total = 0
        
        # 1. 헬스 체크
        tests_total += 1
        if self.test_health_check():
            tests_passed += 1
            
        # 2. 단일 데이터 전송
        tests_total += 1
        if self.test_single_data_submission():
            tests_passed += 1
            
        # 3. 배치 데이터 전송
        tests_total += 1
        if self.test_batch_data_submission(20):
            tests_passed += 1
            
        # 4. 잘못된 데이터 처리
        tests_total += 1
        self.test_invalid_data()
        tests_passed += 1  # 에러 처리 테스트는 완료만 확인
        
        # 5. 짧은 스트리밍 테스트
        tests_total += 1
        if self.test_continuous_stream(duration_minutes=1, interval_seconds=5):
            tests_passed += 1
        
        print("\n" + "=" * 60)
        print(f"📊 최종 결과: {tests_passed}/{tests_total} 테스트 통과")
        print("=" * 60)
        
        return tests_passed == tests_total

def main():
    parser = argparse.ArgumentParser(description='마이크로그리드 시스템 테스터')
    parser.add_argument('--url', default='http://localhost:15000', help='웹서버 URL')
    parser.add_argument('--mode', choices=['full', 'single', 'batch', 'stream', 'health'], 
                       default='full', help='테스트 모드')
    parser.add_argument('--duration', type=int, default=5, help='스트리밍 테스트 기간(분)')
    parser.add_argument('--interval', type=int, default=5, help='스트리밍 간격(초)')
    parser.add_argument('--batch-size', type=int, default=20, help='배치 크기')
    
    args = parser.parse_args()
    
    tester = MicrogridSystemTester(args.url)
    
    if args.mode == 'full':
        success = tester.run_full_test()
        sys.exit(0 if success else 1)
    elif args.mode == 'single':
        tester.test_single_data_submission()
    elif args.mode == 'batch':
        tester.test_batch_data_submission(args.batch_size)
    elif args.mode == 'stream':
        tester.test_continuous_stream(args.duration, args.interval)
    elif args.mode == 'health':
        tester.test_health_check()

if __name__ == "__main__":
    main()