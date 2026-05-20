"""
Cliente gRPC — consulta o servidor de temperatura.
Demonstra: GetLatest, GetHistory e GetStats.
"""

import grpc
import temperature_pb2
import temperature_pb2_grpc
from config import GRPC_HOST, GRPC_PORT


def cabecalho(titulo: str):
    linha = '-' * 48
    print(f"\n{linha}\n {titulo}\n{linha}")


def exibir_leitura(r, prefixo: str = ''):
    print(f"{prefixo}Média:     {r.average:.2f}°C")
    print(f"{prefixo}Mínima:    {r.min_temp:.2f}°C")
    print(f"{prefixo}Máxima:    {r.max_temp:.2f}°C")
    print(f"{prefixo}Amostras:  {r.sample_count}")
    print(f"{prefixo}Timestamp: {r.timestamp}")


def consultar_ultima(stub):
    cabecalho("Última leitura disponível")
    try:
        resp = stub.GetLatest(temperature_pb2.EmptyRequest())
        exibir_leitura(resp)
    except grpc.RpcError as err:
        print(f"  [ERRO] {err.details()}")


def consultar_historico(stub, quantidade: int = 5):
    cabecalho(f"Histórico — últimas {quantidade} médias")
    try:
        resp = stub.GetHistory(temperature_pb2.HistoryRequest(limit=quantidade))
        if not resp.records:
            print("  Nenhum histórico disponível.")
            return
        for idx, reg in enumerate(resp.records, start=1):
            print(f"  [{idx}] {reg.timestamp[:19]} | ", end='')
            print(f"média: {reg.average:.2f}°C | min: {reg.min_temp:.2f}°C | "
                  f"max: {reg.max_temp:.2f}°C | amostras: {reg.sample_count}")
    except grpc.RpcError as err:
        print(f"  [ERRO] {err.details()}")


def consultar_estatisticas(stub):
    cabecalho("Estatísticas gerais")
    try:
        resp = stub.GetStats(temperature_pb2.EmptyRequest())
        print(f"  Média geral:      {resp.overall_avg:.2f}°C")
        print(f"  Mínima registrada:{resp.overall_min:.2f}°C")
        print(f"  Máxima registrada:{resp.overall_max:.2f}°C")
        print(f"  Total de registros: {resp.total_records}")
    except grpc.RpcError as err:
        print(f"  [ERRO] {err.details()}")


def executar():
    endereco = f"{GRPC_HOST}:{GRPC_PORT}"
    print(f"[CLIENTE] Conectando a {endereco}...")

    with grpc.insecure_channel(endereco) as canal:
        stub = temperature_pb2_grpc.TemperatureServiceStub(canal)
        consultar_ultima(stub)
        consultar_historico(stub, quantidade=5)
        consultar_estatisticas(stub)

    print("\n[CLIENTE] Consulta encerrada.\n")


if __name__ == '__main__':
    executar()
