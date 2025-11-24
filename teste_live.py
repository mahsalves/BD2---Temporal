import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.filterwarnings("ignore", category=DeprecationWarning) 
warnings.simplefilter("ignore", MissingPivotFunction)

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import pandas as pd
from tabulate import tabulate



INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "28QxqHAQiHbnnEt0-1xYiEsjhiQJh0lCJgEvuY4zLCweK3b3eMKTesJucuPv-S_Fsepr6A2f8gQHcN4rvL1gVg==" 
INFLUX_ORG = "Seminario_BD2" 
INFLUX_BUCKET = "Monitoramento_temp"

# Inicializa a conexão
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

# inserção simples

def inserir_ponto_ao_vivo(local, sensor_id, valor_temperatura):
    """Insere um único ponto de dado de temperatura no InfluxDB."""
    
    # constrói o ponto
    ponto_novo = (
        Point("temperatura")
        .tag("localizacao", local)
        .tag("sensor_id", sensor_id)
        .field("graus", float(valor_temperatura))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    
    # insere
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=ponto_novo)
    print(f"Ponto {valor_temperatura}°C inserido no sensor {sensor_id} ({local}).")


def verificar_insercao_recente(query_api, sensor_id):
    """Consulta os dados dos últimos 5 minutos filtrando pelo sensor ID."""
    print("\n--- VERIFICANDO INSERCAO RECENTE ---")

    # a consulta flux buscará apenas o ponto que acabou de entrar
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "temperatura")
      |> filter(fn: (r) => r.sensor_id == "{sensor_id}")
      |> yield(name: "ponto_inserido")
    '''

    try:
        result_df = query_api.query_data_frame(org=INFLUX_ORG, query=flux_query)
        
        if result_df.empty:
            print("Aviso: O ponto inserido nao foi encontrado na consulta recente.")
        else:
            print(f"Ponto ENCONTRADO para o sensor {sensor_id}:")
            # exibe o timestamp e o valor
            print(result_df[['_time', '_value']])
            
    except Exception as e:
        print(f"Erro na verificacao: {e}")

# demonstração

if __name__ == "__main__":
    print("\n--- INSERCAO EM TEMPO REAL ---")
    
    local = "auditorio"
    sensor = "LIVE-01"
    valor = 28.5  # valor que deve ser mudado
    
    inserir_ponto_ao_vivo(local, sensor, valor)

    verificar_insercao_recente(query_api, sensor)
    
    print("-----------------------------\n")
    client.close()