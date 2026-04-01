import pandas as pd
import json
import requests
import sys

# Dicionário de Coordenadas das Delegacias da Capital (RJ)
# Mapeia o código da DP (CISP) para Latitude e Longitude
geo_dps = {
    "001": {"lat": -22.8975, "lng": -43.1802}, "004": {"lat": -22.9125, "lng": -43.1883},
    "005": {"lat": -22.9134, "lng": -43.1855}, "006": {"lat": -22.9101, "lng": -43.2012},
    "007": {"lat": -22.9254, "lng": -43.2039}, "009": {"lat": -22.9265, "lng": -43.1782},
    "010": {"lat": -22.9515, "lng": -43.1911}, "012": {"lat": -22.9711, "lng": -43.1844},
    "013": {"lat": -22.9832, "lng": -43.1925}, "014": {"lat": -22.9845, "lng": -43.2231},
    "015": {"lat": -22.9754, "lng": -43.2322}, "017": {"lat": -22.8982, "lng": -43.2215},
    "018": {"lat": -22.9351, "lng": -43.2195}, "019": {"lat": -22.9234, "lng": -43.2355},
    "020": {"lat": -22.9155, "lng": -43.2422}, "021": {"lat": -22.8624, "lng": -43.2544},
    "022": {"lat": -22.8355, "lng": -43.2811}, "023": {"lat": -22.9133, "lng": -43.2711},
    "024": {"lat": -22.8955, "lng": -43.2988}, "025": {"lat": -22.8944, "lng": -43.3244},
    "027": {"lat": -22.8344, "lng": -43.3422}, "031": {"lat": -23.0188, "lng": -43.4611},
    "032": {"lat": -22.9188, "lng": -43.3711}, "033": {"lat": -22.8722, "lng": -43.4211},
    "034": {"lat": -22.8788, "lng": -43.4644}, "035": {"lat": -22.9011, "lng": -43.5611},
    "036": {"lat": -22.9155, "lng": -43.6844}, "037": {"lat": -22.8188, "lng": -43.2044},
    "038": {"lat": -22.8311, "lng": -43.3155}, "039": {"lat": -22.8122, "lng": -43.3444},
    "040": {"lat": -22.8255, "lng": -43.3011}, "041": {"lat": -22.8752, "lng": -43.3411},
    "042": {"lat": -22.8455, "lng": -43.3811}, "043": {"lat": -22.9388, "lng": -43.5411},
    "044": {"lat": -22.8711, "lng": -43.3188}, "016": {"lat": -23.0019, "lng": -43.3444}
}

def download_and_process():
    # URL Direta fornecida
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv"
    print(f"Baixando arquivo direto do ISP: {url}")
    
    try:
        # Tenta o download com cabeçalho de navegador para evitar bloqueios
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, verify=False, timeout=60)
        
        if res.status_code != 200:
            print(f"Erro no download. Status: {res.status_code}")
            sys.exit(1)
            
        with open("temp.csv", "wb") as f:
            f.write(res.content)

        # O arquivo do ISP usa ponto-e-vírgula como separador
        df = pd.read_csv("temp.csv", sep=';', encoding='iso-8859-1', low_memory=False)
        df.columns = [c.lower().strip() for c in df.columns]

        # Pega o ano mais recente presente no arquivo
        ultimo_ano = df['ano'].max()
        print(f"Processando dados consolidados de: {ultimo_ano}")

        # Filtra pelo último ano e cria uma cópia para trabalho
        df_recente = df[df['ano'] == ultimo_ano].copy()

        # Definição das colunas de interesse
        colunas_crime = ['roubo_veiculo', 'roubo_celular', 'roubo_rua', 'hom_doloso']
        
        # Garante conversão numérica
        for col in colunas_crime:
            if col in df_recente.columns:
                df_recente[col] = pd.to_numeric(df_recente[col], errors='coerce').fillna(0)

        heatmap_data = []

        # Agrupa pelo código da delegacia (neste arquivo chama-se 'cisp')
        for cisp, group in df_recente.groupby('cisp'):
            # Formata o código (ex: 5 vira 005)
            cod_dp = str(int(cisp)).zfill(3)

            if cod_dp in geo_dps:
                v_total = int(group[colunas_crime].sum().sum())
                v_veiculos = int(group['roubo_veiculo'].sum())
                v_homicidios = int(group['hom_doloso'].sum())

                if v_total > 0:
                    heatmap_data.append({
                        "lat": geo_dps[cod_dp]["lat"],
                        "lng": geo_dps[cod_dp]["lng"],
                        "total": v_total,
                        "veiculos": v_veiculos,
                        "homicidios": v_homicidios
                    })

        # Salva o arquivo JSON final
        with open('isp_crime_stats.json', 'w', encoding='utf-8') as f:
            json.dump(heatmap_data, f, ensure_ascii=False)

        print(f"Sucesso! {len(heatmap_data)} CISPs processadas.")

    except Exception as e:
        print(f"Erro no processamento: {e}")
        sys.exit(1)

if __name__ == "__main__":
    download_and_process()
