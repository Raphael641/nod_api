import pandas as pd
import json
import requests
import os
import sys

# Dicionário de Delegacias (Completo para Capital)
geo_dps = {
    "001": {"lat": -22.8975, "lng": -43.1802}, "004": {"lat": -22.9125, "lng": -43.1883},
    "005": {"lat": -22.9134, "lng": -43.1855}, "006": {"lat": -22.9101, "lng": -43.2012},
    "007": {"lat": -22.9254, "lng": -43.2039}, "009": {"lat": -22.9265, "lng": -43.1782},
    "010": {"lat": -22.9515, "lng": -43.1911}, "012": {"lat": -22.9711, "lng": -43.1844},
    "013": {"lat": -22.9832, "lng": -43.1925}, "014": {"lat": -22.9845, "lng": -43.2231},
    "015": {"lat": -22.9754, "lng": -43.2322}, "016": {"lat": -23.0019, "lng": -43.3444},
    "017": {"lat": -22.8982, "lng": -43.2215}, "018": {"lat": -22.9351, "lng": -43.2195},
    "019": {"lat": -22.9234, "lng": -43.2355}, "020": {"lat": -22.9155, "lng": -43.2422},
    "021": {"lat": -22.8624, "lng": -43.2544}, "022": {"lat": -22.8355, "lng": -43.2811},
    "023": {"lat": -22.9133, "lng": -43.2711}, "024": {"lat": -22.8955, "lng": -43.2988},
    "025": {"lat": -22.8944, "lng": -43.3244}, "027": {"lat": -22.8344, "lng": -43.3422},
    "031": {"lat": -23.0188, "lng": -43.4611}, "032": {"lat": -22.9188, "lng": -43.3711},
    "033": {"lat": -22.8722, "lng": -43.4211}, "034": {"lat": -22.8788, "lng": -43.4644},
    "035": {"lat": -22.9011, "lng": -43.5611}, "036": {"lat": -22.9155, "lng": -43.6844},
    "037": {"lat": -22.8188, "lng": -43.2044}, "038": {"lat": -22.8311, "lng": -43.3155},
    "039": {"lat": -22.8122, "lng": -43.3444}, "040": {"lat": -22.8255, "lng": -43.3011},
    "041": {"lat": -22.8752, "lng": -43.3411}, "042": {"lat": -22.8455, "lng": -43.3811},
    "043": {"lat": -22.9388, "lng": -43.5411}, "044": {"lat": -22.8711, "lng": -43.3188}
}

def download_and_process():
    # Link Oficial (tentando sem HTTPS primeiro para evitar bloqueio de certificado)
    url = "http://www.ispdados.rj.gov.br/Arquivos/BaseMensalDelegaciaSerieHistorica.csv"
    
    # Cabeçalhos para fingir que somos um navegador Chrome no Windows
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/csv'
    }
    
    print(f"Tentando download via Chrome-Agent: {url}")
    
    try:
        # Tenta baixar com timeout e sem verificar SSL (mais robusto para órgãos públicos)
        response = requests.get(url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            print(f"Falha no link HTTP (Status {response.status_code}). Tentando HTTPS...")
            response = requests.get(url.replace("http://", "https://"), headers=headers, timeout=60, verify=False)

        if response.status_code == 200:
            with open("temp.csv", "wb") as f:
                f.write(response.content)
            print("Arquivo baixado com sucesso!")
        else:
            print(f"O servidor do ISP-RJ retornou erro {response.status_code}. O site pode estar fora do ar.")
            sys.exit(1)

        # Processamento
        df = pd.read_csv("temp.csv", sep=';', encoding='iso-8859-1', low_memory=False)
        df.columns = [c.lower().strip() for c in df.columns]
        
        ultimo_ano = df['ano'].max()
        print(f"Extraindo dados do ano: {ultimo_ano}")
        
        df_recente = df[df['ano'] == ultimo_ano].copy()
        colunas_crime = ['roubo_veiculo', 'roubo_celular', 'roubo_rua', 'hom_doloso']
        
        for col in colunas_crime:
            if col in df_recente.columns:
                df_recente[col] = pd.to_numeric(df_recente[col], errors='coerce').fillna(0)

        heatmap_data = []
        for distrito, group in df_recente.groupby('distrito'):
            cod_dp = str(int(distrito)).zfill(3)
            if cod_dp in geo_dps:
                heatmap_data.append({
                    "lat": geo_dps[cod_dp]["lat"],
                    "lng": geo_dps[cod_dp]["lng"],
                    "total": int(group[colunas_crime].sum().sum()),
                    "veiculos": int(group.get('roubo_veiculo', 0).sum()),
                    "homicidios": int(group.get('hom_doloso', 0).sum())
                })
        
        with open('isp_crime_stats.json', 'w', encoding='utf-8') as f:
            json.dump(heatmap_data, f)
        
        print("Sucesso: Arquivo JSON de inteligência territorial gerado!")

    except Exception as e:
        print(f"Erro fatal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    download_and_process()
