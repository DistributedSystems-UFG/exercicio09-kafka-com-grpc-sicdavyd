"""
Servidor gRPC + Consumidor Kafka
Armazena leituras processadas no SQLite e responde consultas via gRPC.
Duas threads: uma consome Kafka, outra serve gRPC.
"""

import json
import sqlite3
import threading
from concurrent import futures
from datetime import datetime

import grpc
import temperature_pb2
import temperature_pb2_grpc
from kafka import KafkaConsumer
from config import BROKER, TOPICO_TRATADO, GRPC_PORT, CAMINHO_DB

CAMPOS = ('average', 'min_temp', 'max_temp', 'sample_count', 'timestamp')


# ── Banco de dados ────────────────────────────────────────────────────────────

def preparar_banco():
    with sqlite3.connect(CAMINHO_DB) as con:
        con.execute('''
            CREATE TABLE IF NOT EXISTS leituras (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                average      REAL    NOT NULL,
                min_temp     REAL    NOT NULL,
                max_temp     REAL    NOT NULL,
                sample_count INTEGER NOT NULL,
                timestamp    TEXT    NOT NULL
            )
        ''')
    print(f"[SERVIDOR] Banco pronto: {CAMINHO_DB}")


def inserir_leitura(dado: dict):
    with sqlite3.connect(CAMINHO_DB) as con:
        con.execute(
            'INSERT INTO leituras (average, min_temp, max_temp, sample_count, timestamp) '
            'VALUES (?, ?, ?, ?, ?)',
            (dado['average'], dado['min_temp'], dado['max_temp'],
             dado['sample_count'], dado['timestamp']),
        )


def _linha_para_dict(linha: tuple) -> dict:
    return dict(zip(CAMPOS, linha))


def buscar_ultima() -> dict | None:
    with sqlite3.connect(CAMINHO_DB) as con:
        cur = con.execute(
            'SELECT average, min_temp, max_temp, sample_count, timestamp '
            'FROM leituras ORDER BY id DESC LIMIT 1'
        )
        linha = cur.fetchone()
    return _linha_para_dict(linha) if linha else None


def buscar_historico(quantidade: int) -> list:
    with sqlite3.connect(CAMINHO_DB) as con:
        cur = con.execute(
            'SELECT average, min_temp, max_temp, sample_count, timestamp '
            'FROM leituras ORDER BY id DESC LIMIT ?',
            (quantidade,),
        )
        return [_linha_para_dict(r) for r in cur.fetchall()]


def buscar_estatisticas() -> dict | None:
    with sqlite3.connect(CAMINHO_DB) as con:
        cur = con.execute(
            'SELECT AVG(average), MIN(min_temp), MAX(max_temp), COUNT(*) FROM leituras'
        )
        linha = cur.fetchone()
    if linha and linha[3] > 0:
        return {
            'overall_avg':   round(linha[0], 2),
            'overall_min':   round(linha[1], 2),
            'overall_max':   round(linha[2], 2),
            'total_records': linha[3],
        }
    return None


# ── Serviço gRPC ──────────────────────────────────────────────────────────────

def _montar_reply(rec: dict) -> temperature_pb2.TemperatureReply:
    return temperature_pb2.TemperatureReply(
        average      = rec['average'],
        min_temp     = rec['min_temp'],
        max_temp     = rec['max_temp'],
        sample_count = rec['sample_count'],
        timestamp    = rec['timestamp'],
    )


class ServicoTemperatura(temperature_pb2_grpc.TemperatureServiceServicer):

    def GetLatest(self, request, context):
        rec = buscar_ultima()
        if not rec:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Nenhuma leitura disponível.")
            return temperature_pb2.TemperatureReply()
        print(f"[gRPC] GetLatest → {rec['average']}°C @ {rec['timestamp']}")
        return _montar_reply(rec)

    def GetHistory(self, request, context):
        limite  = max(1, min(request.limit, 100))
        registros = buscar_historico(limite)
        print(f"[gRPC] GetHistory → {len(registros)} registros")
        return temperature_pb2.HistoryReply(
            records=[_montar_reply(r) for r in registros]
        )

    def GetStats(self, request, context):
        stats = buscar_estatisticas()
        if not stats:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Sem dados para estatísticas.")
            return temperature_pb2.StatsReply()
        print(
            f"[gRPC] GetStats → avg:{stats['overall_avg']}°C "
            f"min:{stats['overall_min']}°C max:{stats['overall_max']}°C "
            f"total:{stats['total_records']}"
        )
        return temperature_pb2.StatsReply(
            overall_avg   = stats['overall_avg'],
            overall_min   = stats['overall_min'],
            overall_max   = stats['overall_max'],
            total_records = stats['total_records'],
        )


# ── Thread Kafka ──────────────────────────────────────────────────────────────

def loop_consumidor_kafka():
    sub = KafkaConsumer(
        TOPICO_TRATADO,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='grupo-servidor',
    )
    print(f"[SERVIDOR] Aguardando mensagens em '{TOPICO_TRATADO}'...")
    for msg in sub:
        dado = msg.value
        inserir_leitura(dado)
        print(
            f"[SERVIDOR] Gravado → média: {dado['average']}°C | "
            f"amostras: {dado['sample_count']} | ts: {dado['timestamp']}"
        )


# ── Inicialização ─────────────────────────────────────────────────────────────

def iniciar():
    preparar_banco()

    thread_kafka = threading.Thread(target=loop_consumidor_kafka, daemon=True)
    thread_kafka.start()

    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_pb2_grpc.add_TemperatureServiceServicer_to_server(ServicoTemperatura(), srv)
    srv.add_insecure_port(f'[::]:{GRPC_PORT}')
    srv.start()

    print(f"[SERVIDOR] gRPC na porta {GRPC_PORT}...\n")
    try:
        srv.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Encerrado.")
        srv.stop(0)


if __name__ == '__main__':
    iniciar()
