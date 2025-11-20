import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.filterwarnings("ignore", category=DeprecationWarning) 
warnings.simplefilter("ignore", MissingPivotFunction)

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta, UTC
import time
import random

# aqui tem que colocar nossos dados do docker!!!!
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "28QxqHAQiHbnnEt0-1xYiEsjhiQJh0lCJgEvuY4zLCweK3b3eMKTesJucuPv-S_Fsepr6A2f8gQHcN4rvL1gVg=="
INFLUX_ORG = "Seminario_BD2"
INFLUX_BUCKET = "Monitoramento_temp"

print("--- Inicializando Conexão InfluxDB ---")
try:
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()
except Exception as e:
    print(f"ERRO DE CONEXÃO INFLUXDB: Verifique o URL, Token e se o Docker está rodando. Erro: {e}")
    exit()

# inserção

def simular_e_escrever_dados():
    """Simula dados de temperatura (Série Temporal) e os escreve no InfluxDB."""
    print("\n--- 2. Simulação e Ingestão de Dados (Escrita) ---")
    
    # tempo
    current_time = datetime.now(UTC)
    start_time = current_time - timedelta(hours=1)
    
    # gera 100 pontos de dados simulados para dois sensores
    num_points = 100
    interval = (current_time - start_time) / num_points
    points_to_write = []

    for i in range(num_points):
        timestamp = (start_time + interval * i).replace(tzinfo=UTC)

        
        # sensor 1: sala (temperatura mais estável)
        temp_sala = 20.0 + (random.random() * 2) 
        point_sala = (
            Point("temperatura")
            .tag("localizacao", "sala")
            .tag("sensor_id", "S001")
            .field("graus", temp_sala) # campo/valor (dado que muda com o tempo)
            .time(timestamp, WritePrecision.NS) # timestamp (eixo temporal)
        )
        points_to_write.append(point_sala)
        
        # sensor 2: Cozinha (temperatura um pouco mais alta)
        temp_cozinha = 24.0 + (random.random() * 1.5)
        point_cozinha = (
            Point("temperatura")
            .tag("localizacao", "cozinha")
            .tag("sensor_id", "S002")
            .field("graus", temp_cozinha)
            .time(timestamp, WritePrecision.NS)
        )
        points_to_write.append(point_cozinha)
        
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points_to_write)
    print(f"✅ {len(points_to_write)} pontos de dados (série temporal) enviados com sucesso.")
    print("------------------------------------------------------------------")

# consulta e análise

def consultar_e_analisar_dados():
    """Consulta os dados usando Flux para aplicar agregação (downsampling)."""
    print("\n--- 3. Consulta e Análise de Dados (Leitura com Flux) ---")

   # consulta flux: filtra dados da sala e calcula a média a cada 10 minutos
    flux_query = f"""
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "temperatura")
    |> filter(fn: (r) => r.localizacao == "sala")
    |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> yield(name: "media_10m_sala")
    """

    
    try:
        # executa a consulta e converte o resultado em um dataframe do pandas
        result_df = query_api.query_data_frame(org=INFLUX_ORG, query=flux_query)
        
        if result_df.empty:
            print("⚠️ Aviso: Nenhuma série temporal agregada encontrada com a consulta Flux.")
            return

        print("Dados de Série Temporal Agregados pelo InfluxDB (Downsampling):")
        print("---------------------------------------------------------------")
        
        # seleciona e exibe as colunas relevantes do resultado
        if isinstance(result_df, list):
             # trata o caso de múltiplas tabelas retornadas
            result_df = pd.concat(result_df)
            
        print(result_df[['_time', '_value']].to_markdown(index=False))

        print(f"\n✅ Consulta Flux executada. O InfluxDB processou {len(result_df)} pontos agregados.")
        
    except Exception as e:
        print(f"Erro ao executar a consulta Flux. Verifique a sintaxe da query: {e}")

# execução

if __name__ == "__main__":
    
    simular_e_escrever_dados()
    
    # pausa para garantir que o banco processou a escrita
    time.sleep(1) 
    
    consultar_e_analisar_dados()

    print("\n--- Fim da Simulação ---")
    client.close()