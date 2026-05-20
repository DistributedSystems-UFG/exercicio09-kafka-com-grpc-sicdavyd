import os

# Kafka
_KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
_KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
BROKER = f"{_KAFKA_HOST}:{_KAFKA_PORT}"

# Tópicos
TOPICO_BRUTO     = 'raw_temps'       # sensor → processador
TOPICO_TRATADO   = 'processed_temps' # processador → servidor

# gRPC
GRPC_HOST = os.getenv('GRPC_HOST', 'localhost')
GRPC_PORT = os.getenv('GRPC_PORT', '50051')

# Banco
CAMINHO_DB = os.getenv('DB_PATH', 'temperaturas.db')
