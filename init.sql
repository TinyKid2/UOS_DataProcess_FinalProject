-- Create database if not exists
CREATE DATABASE IF NOT EXISTS model_evaluation;
USE model_evaluation;

-- Table for model submissions
CREATE TABLE IF NOT EXISTS submissions (
    id VARCHAR(36) PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    filepath VARCHAR(500) NOT NULL,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    submitter_name VARCHAR(100),
    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
    INDEX idx_submitted_at (submitted_at)
);

-- Table for evaluation results
CREATE TABLE IF NOT EXISTS evaluation_results (
    submission_id VARCHAR(36) PRIMARY KEY,
    f1_score DECIMAL(5,3),
    precision_score DECIMAL(5,3),
    recall_score DECIMAL(5,3),
    map50 DECIMAL(5,3),
    map50_95 DECIMAL(5,3),
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (submission_id) REFERENCES submissions(id),
    INDEX idx_evaluated_at (evaluated_at)
);

-- Table for detailed metrics (optional)
CREATE TABLE IF NOT EXISTS detailed_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    submission_id VARCHAR(36),
    class_name VARCHAR(100),
    precision_val DECIMAL(5,3),
    recall_val DECIMAL(5,3),
    f1_val DECIMAL(5,3),
    support INT,
    FOREIGN KEY (submission_id) REFERENCES submissions(id),
    INDEX idx_submission (submission_id)
);

-- Create user if not exists and grant privileges
-- This is handled by Docker environment variables

-- Sample data for testing
INSERT INTO submissions (id, filename, filepath, submitter_name) VALUES
('test-001', 'test_model.pt', '/uploads/test_model.pt', 'Test User')
ON DUPLICATE KEY UPDATE filename=filename;

INSERT INTO evaluation_results (submission_id, f1_score, precision_score, recall_score, map50, map50_95) VALUES
('test-001', 0.850, 0.870, 0.830, 0.890, 0.650)
ON DUPLICATE KEY UPDATE f1_score=f1_score;