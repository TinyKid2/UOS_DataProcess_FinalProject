import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
import pymysql
from datetime import datetime
import random
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'etl_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'etl_password')
DB_NAME = os.environ.get('DB_NAME', 'model_evaluation')

class ModelEvaluator:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.setup_kafka()
        
    def setup_kafka(self):
        """Initialize Kafka consumer and producer"""
        try:
            self.consumer = KafkaConsumer(
                'model_submission',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='evaluation-group'
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka connections established")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def evaluate_model(self, model_path):
        """
        Evaluate YOLO model
        In production, this would use ultralytics YOLO
        For demo, we'll simulate evaluation
        """
        try:
            # Simulate model loading and evaluation
            logger.info(f"Evaluating model: {model_path}")
            
            # In production:
            # from ultralytics import YOLO
            # model = YOLO(model_path)
            # results = model.val()
            
            # Simulated metrics for demo
            time.sleep(2)  # Simulate processing time
            metrics = {
                'f1': round(random.uniform(0.7, 0.95), 3),
                'precision': round(random.uniform(0.75, 0.98), 3),
                'recall': round(random.uniform(0.65, 0.92), 3),
                'mAP50': round(random.uniform(0.70, 0.95), 3),
                'mAP50-95': round(random.uniform(0.50, 0.80), 3)
            }
            
            logger.info(f"Evaluation complete: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error evaluating model: {e}")
            return None
    
    def save_to_database(self, submission_id, metrics):
        """Save evaluation results to database"""
        try:
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO evaluation_results 
                (submission_id, f1_score, precision_score, recall_score, map50, map50_95, evaluated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                f1_score = VALUES(f1_score),
                precision_score = VALUES(precision_score),
                recall_score = VALUES(recall_score),
                map50 = VALUES(map50),
                map50_95 = VALUES(map50_95),
                evaluated_at = VALUES(evaluated_at)
            """, (
                submission_id,
                metrics['f1'],
                metrics['precision'],
                metrics['recall'],
                metrics['mAP50'],
                metrics['mAP50-95'],
                datetime.now()
            ))
            
            conn.commit()
            conn.close()
            logger.info(f"Results saved to database for {submission_id}")
            return True
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return False
    
    def process_submission(self, message):
        """Process a single model submission"""
        try:
            submission_id = message['id']
            filepath = message['filepath']
            
            logger.info(f"Processing submission {submission_id}")
            
            # Evaluate model
            metrics = self.evaluate_model(filepath)
            
            if metrics:
                # Save to database
                self.save_to_database(submission_id, metrics)
                
                # Send results to Kafka
                result_message = {
                    'id': submission_id,
                    'filename': message['filename'],
                    'metrics': metrics,
                    'evaluated_at': datetime.now().isoformat(),
                    'status': 'completed'
                }
                
                self.producer.send('eval_result', result_message)
                self.producer.flush()
                
                logger.info(f"Evaluation completed for {submission_id}")
            else:
                logger.error(f"Failed to evaluate model {submission_id}")
                
        except Exception as e:
            logger.error(f"Error processing submission: {e}")
            traceback.print_exc()
    
    def run(self):
        """Main processing loop"""
        logger.info("Evaluation node started, waiting for submissions...")
        
        try:
            for message in self.consumer:
                self.process_submission(message.value)
        except KeyboardInterrupt:
            logger.info("Shutting down evaluation node...")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            traceback.print_exc()
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

if __name__ == "__main__":
    evaluator = ModelEvaluator()
    evaluator.run()