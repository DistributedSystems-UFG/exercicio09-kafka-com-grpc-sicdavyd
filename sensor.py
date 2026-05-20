"""
Sensor simulado — Produtor Kafka
Gera leituras de temperatura e publica no tópico de dados brutos
sempre que houver variação relevante.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from config import BROKER, TOPICO_BRUTO

TEMPERATURA_INICIAL = 22.0
TEMP_MINIMA         = 15.0
TEMP_MAXIMA         = 35.0
INTERVALO_LEITURA   = 1.0   # segundos


def nova_leitura(temp_atual: float) -> float:
    variacao = random.uniform(-1.0, 1.0)
    nova = round(temp_atual + variacao, 2)
    return max(TEMP_MINIMA, min(TEMP_MAXIMA, nova))


def montar_payload(temp: float) -> dict:
    return {
        'temperature': temp,
        'timestamp': datetime.now().isoformat(),
    }


def iniciar_sensor():
    pub = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda dado: json.dumps(dado).encode('utf-8'),
    )

    print(f"[SENSOR] Ativo. Publicando em '{TOPICO_BRUTO}' | Broker: {BROKER}")

    temp_atual = TEMPERATURA_INICIAL
    try:
        while True:
            temp_atual = nova_leitura(temp_atual)
            payload    = montar_payload(temp_atual)

            pub.send(TOPICO_BRUTO, value=payload)
            print(f"[SENSOR] Publicado: {temp_atual}°C às {payload['timestamp'][11:19]}")

            time.sleep(INTERVALO_LEITURA)

    except KeyboardInterrupt:
        print("\n[SENSOR] Interrompido.")
    finally:
        pub.close()


if __name__ == '__main__':
    iniciar_sensor()
