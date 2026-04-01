import pandas as pd
import json
import os

# Dicionário de Coordenadas das Delegacias da Capital (RJ)
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
    "027": {"lat": -22.8344, "lng": -43.3422}, "037": {"lat": -22.8188, "lng": -43.2044},
    "038": {"lat": -22.8311, "lng": -43.3155}, "039": {"lat": -22.8122, "lng": -43.3444},
    "040": {"lat": -22.8255, "lng": -43.3011}, "041": {"lat": -22.8752, "lng": -43.3411},
    "044": {"lat": -22.8711, "lng": -43.3188}, "016": {"lat": -23.0019, "lng": -43.3444},
    "031": {"lat": -23.0188, "lng": -43.4611}, "032": {"lat": -22.9188, "lng": -43.3711},
    "033": {"lat": -22.8722, "lng": -43.4211}, "034": {"lat": -22.8788, "lng": -43.4644},
    "035": {"lat": -22.9011, "lng": -43.5611}, "036": {"lat": -22.9155, "lng": -43.6844},
    "042": {"lat": -22.8455, "lng": -43.3811}, "043": {"lat": -22.9388, "lng": -43.5411}
}

def download_and_process():
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseMensalDelegaciaSerieHistorica.csv"
    print(f"Iniciando download do ISP-RJ: {url}")
    
    try:
        # Baixa e lê o CSV
        df = pd.read_csv(url, sep=';', encoding='iso-8859-1', low_memory=False)
        df.columns = [c.lower().strip() for c in df.columns]
        
        # INTELIGÊNCIA APLICADA: Descobre os dois anos mais recentes na planilha
        anos_disponiveis = df['ano'].dropna().unique()
        anos_disponiveis.sort() # Ordena do menor pro maior
        anos_recentes = anos_disponiveis[-2:] # Pega os dois últimos (ex: 2024 e 2025)
        
        print(f"Anos encontrados na base do Governo: {anos_recentes}")
        
        df_recente = df[df['ano'].isin(anos_recentes)].copy()
        
        colunas_analise =['roubo_veiculo', 'roubo_celular', 'roubo_rua', 'hom_doloso']
        
        # Converte para numérico e preenche vazios com 0
        for col in colunas_analise:
            if col in df_recente.columns:
                df_recente[col] = pd.to_numeric(df_recente[col], errors='coerce').fillna(0)
            else:
                df_recente[col] = 0 # Se a coluna sumir do ISP, não quebra o código

        heatmap_data =[]
        
        # Agrupa por delegacia
        for distrito, group in df_recente.groupby('distrito'):
            cod_dp = str(int(distrito)).zfill(3)
            
            if cod_dp in geo_dps:
                total_crimes = group[colunas_analise].sum().sum()
                roubos_veiculos = group['roubo_veiculo'].sum()
                homicidios = group['hom_doloso'].sum()
                
                if total_crimes > 0:
                    heatmap_data.append({
                        "lat": geo_dps[cod_dp]["lat"],
                        "lng": geo_dps[cod_dp]["lng"],
                        "total": int(total_crimes),
                        "veiculos": int(roubos_veiculos),
                        "homicidios": int(homicidios)
                    })
        
        output_file = 'isp_crime_stats.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(heatmap_data, f, ensure_ascii=False)
        
        print(f"Sucesso! {len(heatmap_data)} DPs processadas e salvas.")

    except Exception as e:
        print(f"ERRO CRÍTICO NO PROCESSAMENTO: {str(e)}")
        exit(1)

if __name__ == "__main__":
    download_and_process()
