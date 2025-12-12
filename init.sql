-- Create database if not exists
CREATE DATABASE IF NOT EXISTS microgrid_control;
USE microgrid_control;

-- 전력 제어 결과 테이블
CREATE TABLE IF NOT EXISTS control_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    result_id VARCHAR(100) UNIQUE,
    request_id VARCHAR(100),
    location VARCHAR(50),
    timestamp DATETIME,
    original_timestamp DATETIME,
    solar_power FLOAT,
    wind_power FLOAT,
    total_power FLOAT,
    battery_charge_rate FLOAT,
    battery_discharge_rate FLOAT,
    battery_soc FLOAT,
    grid_import FLOAT,
    grid_export FLOAT,
    load_shedding FLOAT,
    renewable_curtailment FLOAT,
    estimated_cost FLOAT,
    efficiency_score FLOAT,
    renewable_utilization FLOAT,
    grid_dependency FLOAT,
    electricity_price FLOAT,
    quality_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_location (location),
    INDEX idx_timestamp (timestamp),
    INDEX idx_efficiency (efficiency_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 시간별 집계 테이블
CREATE TABLE IF NOT EXISTS hourly_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(50),
    hour_timestamp DATETIME,
    avg_solar_power FLOAT,
    avg_wind_power FLOAT,
    total_energy_generated FLOAT,
    total_battery_charged FLOAT,
    total_battery_discharged FLOAT,
    total_grid_import FLOAT,
    total_grid_export FLOAT,
    avg_efficiency_score FLOAT,
    total_cost FLOAT,
    record_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_location_hour (location, hour_timestamp),
    INDEX idx_hour (hour_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 일별 집계 테이블
CREATE TABLE IF NOT EXISTS daily_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(50),
    date DATE,
    total_solar_energy FLOAT,
    total_wind_energy FLOAT,
    total_energy_generated FLOAT,
    total_energy_consumed FLOAT,
    total_grid_import FLOAT,
    total_grid_export FLOAT,
    avg_efficiency_score FLOAT,
    max_efficiency_score FLOAT,
    min_efficiency_score FLOAT,
    total_cost FLOAT,
    total_revenue FLOAT,
    net_profit FLOAT,
    peak_power FLOAT,
    min_power FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_location_date (location, date),
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 알람 및 이벤트 테이블
CREATE TABLE IF NOT EXISTS system_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(50),
    severity VARCHAR(20),
    location VARCHAR(50),
    description TEXT,
    event_data JSON,
    timestamp DATETIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event_type (event_type),
    INDEX idx_severity (severity),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 마이크로그리드 정보 테이블
CREATE TABLE IF NOT EXISTS microgrids (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(50) UNIQUE,
    name VARCHAR(100),
    battery_capacity FLOAT DEFAULT 5000,
    battery_max_charge FLOAT DEFAULT 1000,
    battery_max_discharge FLOAT DEFAULT 1000,
    grid_max_import FLOAT DEFAULT 3000,
    grid_max_export FLOAT DEFAULT 3000,
    peak_load FLOAT DEFAULT 5000,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 샘플 마이크로그리드 데이터 삽입
INSERT INTO microgrids (location, name, battery_capacity, latitude, longitude) VALUES
('MicroGrid-01', '서울 상업지구 마이크로그리드', 5000, 37.5665, 126.9780),
('MicroGrid-02', '대전 산업단지 마이크로그리드', 8000, 36.3504, 127.3845),
('MicroGrid-03', '부산 주거지구 마이크로그리드', 3000, 35.1796, 129.0756)
ON DUPLICATE KEY UPDATE name=VALUES(name);