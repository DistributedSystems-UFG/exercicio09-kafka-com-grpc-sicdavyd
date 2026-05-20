"""
Processador — Consumidor/Produtor Kafka
Lê leituras brutas, mantém uma janela deslizante de tempo,
e publica estatísticas agregadas quando há amostras suficientes.
"""

import json
from collections import deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from config import BROKER, TOPICO_BRUTO, TOPICO_TRATADO

JANELA_SEGUNDOS  = 120
MINIMO_AMOSTRAS  = 2


def criar_consumidor() -> KafkaConsumer:
    return KafkaConsumer(
        TOPICO_BRUTO,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='grupo-processador',
    )


def criar_produtor() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )


def remover_expirados(janela: deque, referencia: datetime) -> None:
    corte = referencia - timedelta(seconds=JANELA_SEGUNDOS)
    while janela and janela[0][0] < corte:
        janela.popleft()


def calcular_estatisticas(janela: deque) -> dict:
    valores = [t for _, t in janela]
    return {
        'average':        round(sum(valores) / len(valores), 2),
        'min_temp':       round(min(valores), 2),
        'max_temp':       round(max(valores), 2),
        'sample_count':   len(valores),
        'window_seconds': JANELA_SEGUNDOS,
        'timestamp':      datetime.now().isoformat(),
    }


def executar():
    consumidor = criar_consumidor()
    produtor   = criar_produtor()
    janela: deque = deque()

    print(f"[PROCESSADOR] Ativo.")
    print(f"[PROCESSADOR] {TOPICO_BRUTO} → {TOPICO_TRATADO}")
    print(f"[PROCESSADOR] Janela: {JANELA_SEGUNDOS}s | Amostras mínimas: {MINIMO_AMOSTRAS}\n")

    try:
        for evento in consumidor:
            dado = evento.value
            temp = dado['temperature']
            ts   = datetime.fromisoformat(dado['timestamp'])

            print(f"[PROCESSADOR] Recebido: {temp}°C às {ts.strftime('%H:%M:%S')}")

            janela.append((ts, temp))
            remover_expirados(janela, ts)

            if len(janela) >= MINIMO_AMOSTRAS:
                stats = calcular_estatisticas(janela)
                produtor.send(TOPICO_TRATADO, value=stats)
                produtor.flush()
                print(
                    f"[PROCESSADOR] Publicado → "
                    f"média: {stats['average']}°C | "
                    f"min: {stats['min_temp']}°C | "
                    f"max: {stats['max_temp']}°C | "
                    f"amostras: {stats['sample_count']}\n"
                )

    except KeyboardInterrupt:
        print("\n[PROCESSADOR] Interrompido.")
    finally:
        consumidor.close()
        produtor.close()


if __name__ == '__main__':
    executar()
