import pandas as pd
import json
import requests
import sys

# Dicionário Geográfico Expandido (Coordenadas Centrais das CISPs)
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
    "041": {"lat": -22.8752, "lng": -43.3411}, "044": {"lat": -22.8711, "lng": -43.3188}
}

def process_isp():
    # Usando a base de CISP (Delegacias) para maior precisão de bairros
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        res = requests.get(url, headers=headers, verify=False)
        with open("temp.csv", "wb") as f:
            f.write(res.content)
            
        df = pd.read_csv("temp.csv", sep=';', encoding='iso-8859-1')
        df.columns = [c.lower().strip() for c in df.columns]

        # Filtro: Últimos 12 meses disponíveis
        ultimo_ano = df['ano'].max()
        df = df[df['ano'] == ultimo_ano].copy()

        # Definição das colunas para os filtros operacionais de Drones
        col_pedestres = ['roubo_transeunte', 'roubo_celular', 'furto_transeunte', 'furto_celular']
        col_veiculos = ['roubo_veiculo', 'furto_veiculos']
        col_violencia = ['hom_doloso', 'tentat_hom', 'lesao_corp_dolosa']

        heatmap_data = []

        for cisp, group in df.groupby('cisp'):
            cod_dp = str(int(cisp)).zfill(3)
            
            if cod_dp in geo_dps:
                # Cálculo de Pesos por Categoria
                p_pedestres = group[col_pedestres].sum().sum()
                p_veiculos = group[col_veiculos].sum().sum()
                p_violencia = group[col_violencia].sum().sum()
                p_total = p_pedestres + p_veiculos + p_violencia

                if p_total > 0:
                    heatmap_data.append({
                        "lat": geo_dps[cod_dp]["lat"],
                        "lng": geo_dps[cod_dp]["lng"],
                        "pedestres": int(p_pedestres),
                        "veiculos": int(p_veiculos),
                        "violencia": int(p_violencia),
                        "total": int(p_total)
                    })

        with open('isp_crime_stats.json', 'w', encoding='utf-8') as f:
            json.dump(heatmap_data, f)
            
        print(f"Sucesso: {len(heatmap_data)} regiões processadas estatisticamente.")

    except Exception as e:
        print(f"Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    process_isp()
