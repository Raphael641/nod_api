import pandas as pd
import json
import requests

# Dicionário de Coordenadas das Delegacias (Mesmo do seu geo_delegacias.php)
geo_dps = {
    "001": {"lat": -22.8975, "lng": -43.1802}, "005": {"lat": -22.9134, "lng": -43.1855},
    "012": {"lat": -22.9711, "lng": -43.1844}, "031": {"lat": -22.9972, "lng": -43.3601},
    "041": {"lat": -22.8512, "lng": -43.3323}, "016": {"lat": -23.0019, "lng": -43.3444},
    # Adicione todas as outras chaves aqui conforme o seu geo_delegacias.php
}

def download_and_process():
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseMensalDelegaciaSerieHistorica.csv"
    print("Baixando CSV do ISP...")
    
    # Baixa e lê o CSV usando pandas (muito mais rápido)
    df = pd.read_csv(url, sep=';', encoding='iso-8859-1')
    
    # Filtra apenas os anos recentes (Ex: 2024 e 2025)
    df_recente = df[df['ano'].isin([2024, 2025])]
    
    crimes_foco = ['roubo_veiculo', 'roubo_celular', 'roubo_rua', 'hom_doloso']
    
    heatmap_data = []
    
    # Agrupa por delegacia (distrito)
    for distrito, group in df_recente.groupby('distrito'):
        cod_dp = str(distrito).zfill(3)
        
        if cod_dp in geo_dps:
            total_crimes = group[crimes_foco].sum().sum()
            if total_crimes > 0:
                heatmap_data.append({
                    "lat": geo_dps[cod_dp]["lat"],
                    "lng": geo_dps[cod_dp]["lng"],
                    "count": int(total_crimes)
                })
    
    # Salva o arquivo leve
    with open('isp_crime_stats.json', 'w') as f:
        json.dump(heatmap_data, f)
    
    print(f"Sucesso! {len(heatmap_data)} pontos processados.")

if __name__ == "__main__":
    download_and_process()
