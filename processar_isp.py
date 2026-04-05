import pandas as pd
import json
import requests
import sys
from datetime import datetime

# Coordenadas detalhadas por bairro/CISP para máxima precisão
geo_bairros = {
    "Centro": {"lat": -22.9068, "lng": -43.1729},
    "Copacabana": {"lat": -22.9675, "lng": -43.1827},
    "Ipanema": {"lat": -22.9845, "lng": -43.2039},
    "Leblon": {"lat": -22.9832, "lng": -43.2225},
    "Botafogo": {"lat": -22.9504, "lng": -43.1866},
    "Flamengo": {"lat": -22.9324, "lng": -43.1722},
    "Tijuca": {"lat": -22.9244, "lng": -43.2312},
    "Méier": {"lat": -22.9040, "lng": -43.2745},
    "Madureira": {"lat": -22.8722, "lng": -43.3411},
    "Campo Grande": {"lat": -22.9030, "lng": -43.5611},
    "Santa Cruz": {"lat": -22.9155, "lng": -43.6844},
    "Bangu": {"lat": -22.8788, "lng": -43.4644},
    "Realengo": {"lat": -22.8722, "lng": -43.4211},
    "Jacarepaguá": {"lat": -22.9388, "lng": -43.3411},
    "Barra da Tijuca": {"lat": -22.9988, "lng": -43.3633},
    "Recreio": {"lat": -23.0188, "lng": -43.4611},
    "Penha": {"lat": -22.8455, "lng": -43.2811},
    "Olaria": {"lat": -22.8555, "lng": -43.2911},
    "Ilha do Governador": {"lat": -22.8122, "lng": -43.2144},
    "Ramos": {"lat": -22.8311, "lng": -43.2355},
    "São Cristóvão": {"lat": -22.8982, "lng": -43.2215},
    "Vila Isabel": {"lat": -22.9134, "lng": -43.2155},
    "Engenho Novo": {"lat": -22.9045, "lng": -43.2655},
    "Piedade": {"lat": -22.8955, "lng": -43.2588},
    "Inhaúma": {"lat": -22.8624, "lng": -43.2844},
    "Del Castilho": {"lat": -22.8855, "lng": -43.2711},
    "Pavuna": {"lat": -22.8255, "lng": -43.3711},
    "Acari": {"lat": -22.8311, "lng": -43.3444},
    "Vigário Geral": {"lat": -22.8355, "lng": -43.3011},
    "Cidade de Deus": {"lat": -22.9388, "lng": -43.3911},
    "Rocinha": {"lat": -22.9888, "lng": -43.2511},
    "Vidigal": {"lat": -22.9955, "lng": -43.2411},
    "Santa Teresa": {"lat": -22.9211, "lng": -43.1811},
    "Laranjeiras": {"lat": -22.9388, "lng": -43.1811},
    "Cosme Velho": {"lat": -22.9311, "lng": -43.1911},
    "Lagoa": {"lat": -22.9711, "lng": -43.2111},
    "Gávea": {"lat": -22.9811, "lng": -43.2311},
    "Jardim Botânico": {"lat": -22.9675, "lng": -43.2211},
    "Urca": {"lat": -22.9515, "lng": -43.1611},
    "Humaitá": {"lat": -22.9555, "lng": -43.1911},
    "Leme": {"lat": -22.9611, "lng": -43.1711}
}

def process():
    """
    Processa dados oficiais do ISP-RJ e gera JSON com estatísticas criminais
    por região para visualização em mapa de calor
    """
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv"
    
    try:
        print(f"[{datetime.now()}] 🔄 Iniciando processamento de dados criminais ISP-RJ...")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, verify=False, timeout=120)
        res.raise_for_status()
        
        with open("temp.csv", "wb") as f:
            f.write(res.content)
        
        print(f"[{datetime.now()}] ✅ Arquivo CSV baixado com sucesso ({len(res.content)} bytes)")
        
        # Ler CSV com tratamento de encoding
        df = pd.read_csv("temp.csv", sep=';', encoding='iso-8859-1', low_memory=False)
        df.columns = [c.lower().strip() for c in df.columns]
        
        print(f"[{datetime.now()}] 📊 Dataset carregado: {len(df)} registros")
        
        # Usar dados do último ano completo para análise
        ultimo_ano = df['ano'].max()
        df_recente = df[df['ano'] == ultimo_ano].copy()
        
        print(f"[{datetime.now()}] 📅 Analisando ano: {ultimo_ano} ({len(df_recente)} registros)")
        
        # Categorias criminais baseadas nas necessidades de inteligência
        # Roubos a pedestres (mais relevante para patrulhamento urbano)
        cols_roubos_pedestres = ['roubo_transeunte', 'roubo_celular', 'furto_transeunte', 'furto_celular']
        
        # Roubos de veículos (para apoio em operações de trânsito)
        cols_roubos_veiculos = ['roubo_veiculo', 'furto_veiculo']
        
        # Crimes violentos/letalidade (para áreas de alto risco)
        cols_violentos = ['hom_doloso', 'tentat_hom', 'lesao_corp_dolosa', 'latrocinio']
        
        # Verificar e converter colunas existentes
        for col in (cols_roubos_pedestres + cols_roubos_veiculos + cols_violentos):
            if col in df_recente.columns:
                df_recente[col] = pd.to_numeric(df_recente[col], errors='coerce').fillna(0)
            else:
                print(f"[{datetime.now()}] ⚠️ Coluna não encontrada: {col}")
                df_recente[col] = 0
        
        # Agrupar por CISP (Circunscrição Integrada de Segurança Pública)
        heatmap_data = []
        
        for cisp, group in df_recente.groupby('cisp'):
            cod_dp = str(int(cisp)).zfill(3)
            
            # Tentar encontrar o bairro correspondente ou usar coordenadas padrão
            # Usamos um mapping básico (pode ser expandido)
            if cod_dp in geo_mapping:
                coords = geo_mapping[cod_dp]
            else:
                # Coordenada padrão (Centro do Rio)
                coords = {"lat": -22.9068, "lng": -43.1729}
            
            heatmap_data.append({
                "lat": coords["lat"],
                "lng": coords["lng"],
                "pedestres": int(group[cols_roubos_pedestres].sum().sum()),
                "veiculos": int(group[cols_roubos_veiculos].sum().sum()),
                "letalidade": int(group[cols_violentos].sum().sum()),
                "total": int(group[cols_roubos_pedestres + cols_roubos_veiculos + cols_violentos].sum().sum()),
                "cisp": cod_dp
            })
        
        # Salvar arquivo JSON
        output_file = 'isp_crime_stats.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(heatmap_data, f, ensure_ascii=False, indent=2)
        
        print(f"[{datetime.now()}] ✅ Mancha criminal gerada com sucesso!")
        print(f"[{datetime.now()}] 📁 Arquivo: {output_file}")
        print(f"[{datetime.now()}] 📍 Regiões processadas: {len(heatmap_data)}")
        
        # Estatísticas resumidas
        total_pedestres = sum(item['pedestres'] for item in heatmap_data)
        total_veiculos = sum(item['veiculos'] for item in heatmap_data)
        total_letalidade = sum(item['letalidade'] for item in heatmap_data)
        
        print(f"\n📊 RESUMO CRIMINAL {ultimo_ano}:")
        print(f"  👥 Roubos a Pedestres: {total_pedestres:,}")
        print(f"  🚗 Roubos de Veículos: {total_veiculos:,}")
        print(f"  ⚠️  Homicídios/Tentativas: {total_letalidade:,}")
        print(f"  📈 Total Geral: {total_pedestres + total_veiculos + total_letalidade:,}")
        
        return True
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ ERRO CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# Mapeamento CISP para coordenadas (baseado em dados reais)
geo_mapping = {
    "001": {"lat": -22.8975, "lng": -43.1802},
    "004": {"lat": -22.9125, "lng": -43.1883},
    "005": {"lat": -22.9134, "lng": -43.1855},
    "006": {"lat": -22.9101, "lng": -43.2012},
    "007": {"lat": -22.9254, "lng": -43.2039},
    "009": {"lat": -22.9265, "lng": -43.1782},
    "010": {"lat": -22.9515, "lng": -43.1911},
    "012": {"lat": -22.9711, "lng": -43.1844},
    "013": {"lat": -22.9832, "lng": -43.1925},
    "014": {"lat": -22.9845, "lng": -43.2231},
    "015": {"lat": -22.9754, "lng": -43.2322},
    "016": {"lat": -23.0019, "lng": -43.3444},
    "017": {"lat": -22.8982, "lng": -43.2215},
    "018": {"lat": -22.9351, "lng": -43.2195},
    "019": {"lat": -22.9234, "lng": -43.2355},
    "020": {"lat": -22.9155, "lng": -43.2422},
    "021": {"lat": -22.8624, "lng": -43.2544},
    "022": {"lat": -22.8355, "lng": -43.2811},
    "023": {"lat": -22.9133, "lng": -43.2711},
    "024": {"lat": -22.8955, "lng": -43.2988},
    "025": {"lat": -22.8944, "lng": -43.3244},
    "027": {"lat": -22.8344, "lng": -43.3422},
    "031": {"lat": -23.0188, "lng": -43.4611},
    "032": {"lat": -22.9188, "lng": -43.3711},
    "033": {"lat": -22.8722, "lng": -43.4211},
    "034": {"lat": -22.8788, "lng": -43.4644},
    "035": {"lat": -22.9011, "lng": -43.5611},
    "036": {"lat": -22.9155, "lng": -43.6844},
    "037": {"lat": -22.8188, "lng": -43.2044},
    "038": {"lat": -22.8311, "lng": -43.3155},
    "039": {"lat": -22.8122, "lng": -43.3444},
    "040": {"lat": -22.8255, "lng": -43.3011},
    "041": {"lat": -22.8752, "lng": -43.3411},
    "042": {"lat": -22.8455, "lng": -43.3811},
    "043": {"lat": -22.9388, "lng": -43.5411},
    "044": {"lat": -22.8711, "lng": -43.3188}
}

if __name__ == "__main__":
    success = process()
    sys.exit(0 if success else 1)
