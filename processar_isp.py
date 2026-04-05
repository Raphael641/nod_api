import pandas as pd
import json
import requests
import sys
from datetime import datetime
import os

# Mapeamento CISP para coordenadas (baseado em dados reais das delegacias)
geo_mapping = {
    "001": {"lat": -22.8975, "lng": -43.1802, "bairro": "Centro"},
    "004": {"lat": -22.9125, "lng": -43.1883, "bairro": "Lapa"},
    "005": {"lat": -22.9134, "lng": -43.1855, "bairro": "Gamboa"},
    "006": {"lat": -22.9101, "lng": -43.2012, "bairro": "Cidade Nova"},
    "007": {"lat": -22.9254, "lng": -43.2039, "bairro": "São Cristóvão"},
    "009": {"lat": -22.9265, "lng": -43.1782, "bairro": "Catumbi"},
    "010": {"lat": -22.9515, "lng": -43.1911, "bairro": "Botafogo"},
    "012": {"lat": -22.9711, "lng": -43.1844, "bairro": "Copacabana"},
    "013": {"lat": -22.9832, "lng": -43.1925, "bairro": "Ipanema"},
    "014": {"lat": -22.9845, "lng": -43.2231, "bairro": "Leblon"},
    "015": {"lat": -22.9754, "lng": -43.2322, "bairro": "Gávea"},
    "016": {"lat": -23.0019, "lng": -43.3444, "bairro": "Barra da Tijuca"},
    "017": {"lat": -22.8982, "lng": -43.2215, "bairro": "São Cristóvão"},
    "018": {"lat": -22.9351, "lng": -43.2195, "bairro": "Tijuca"},
    "019": {"lat": -22.9234, "lng": -43.2355, "bairro": "Méier"},
    "020": {"lat": -22.9155, "lng": -43.2422, "bairro": "Abolição"},
    "021": {"lat": -22.8624, "lng": -43.2544, "bairro": "Inhaúma"},
    "022": {"lat": -22.8355, "lng": -43.2811, "bairro": "Penha"},
    "023": {"lat": -22.9133, "lng": -43.2711, "bairro": "Engenho Novo"},
    "024": {"lat": -22.8955, "lng": -43.2988, "bairro": "Piedade"},
    "025": {"lat": -22.8944, "lng": -43.3244, "bairro": "Madureira"},
    "027": {"lat": -22.8344, "lng": -43.3422, "bairro": "Irajá"},
    "031": {"lat": -23.0188, "lng": -43.4611, "bairro": "Recreio"},
    "032": {"lat": -22.9188, "lng": -43.3711, "bairro": "Jacarepaguá"},
    "033": {"lat": -22.8722, "lng": -43.4211, "bairro": "Realengo"},
    "034": {"lat": -22.8788, "lng": -43.4644, "bairro": "Bangu"},
    "035": {"lat": -22.9011, "lng": -43.5611, "bairro": "Campo Grande"},
    "036": {"lat": -22.9155, "lng": -43.6844, "bairro": "Santa Cruz"},
    "037": {"lat": -22.8188, "lng": -43.2044, "bairro": "Ilha do Governador"},
    "038": {"lat": -22.8311, "lng": -43.3155, "bairro": "Brás de Pina"},
    "039": {"lat": -22.8122, "lng": -43.3444, "bairro": "Pavuna"},
    "040": {"lat": -22.8255, "lng": -43.3011, "bairro": "Vigário Geral"},
    "041": {"lat": -22.8752, "lng": -43.3411, "bairro": "Rocha Miranda"},
    "042": {"lat": -22.8455, "lng": -43.3811, "bairro": "Coelho Neto"},
    "043": {"lat": -22.9388, "lng": -43.5411, "bairro": "Guaratiba"},
    "044": {"lat": -22.8711, "lng": -43.3188, "bairro": "Marechal Hermes"}
}

def process():
    """
    Processa dados oficiais do ISP-RJ e gera JSON com estatísticas criminais
    por região para visualização em mapa de calor
    """
    url = "https://www.ispdados.rj.gov.br/Arquivos/BaseDPEvolucaoMensalCisp.csv"
    
    try:
        print(f"[{datetime.now()}] 🔄 Iniciando processamento de dados criminais ISP-RJ...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/csv,application/csv',
            'Accept-Language': 'pt-BR,pt;q=0.9'
        }
        
        # Baixar o arquivo CSV
        print(f"[{datetime.now()}] 📥 Baixando dados de: {url}")
        res = requests.get(url, headers=headers, verify=False, timeout=120)
        res.raise_for_status()
        
        # Salvar temporariamente
        temp_file = "temp_isp_data.csv"
        with open(temp_file, "wb") as f:
            f.write(res.content)
        
        print(f"[{datetime.now()}] ✅ CSV baixado com sucesso ({len(res.content):,} bytes)")
        
        # Tentar diferentes encodings
        encodings = ['iso-8859-1', 'latin1', 'utf-8', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(temp_file, sep=';', encoding=encoding, low_memory=False)
                print(f"[{datetime.now()}] ✅ Leitura bem sucedida com encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise Exception("Não foi possível ler o CSV com nenhum encoding")
        
        # Limpar nomes das colunas
        df.columns = [c.lower().strip() for c in df.columns]
        
        print(f"[{datetime.now()}] 📊 Dataset carregado: {len(df):,} registros")
        print(f"[{datetime.now()}] 📋 Colunas disponíveis: {list(df.columns)[:10]}...")
        
        # Usar dados dos últimos 12 meses para análise mais recente
        if 'ano' in df.columns and 'mes' in df.columns:
            ultimo_ano = df['ano'].max()
            ultimo_mes = df[df['ano'] == ultimo_ano]['mes'].max()
            df_recente = df[(df['ano'] == ultimo_ano) & (df['mes'] == ultimo_mes)].copy()
            print(f"[{datetime.now()}] 📅 Analisando período: {ultimo_ano}/{ultimo_mes} ({len(df_recente):,} registros)")
        else:
            # Fallback: últimos registros
            df_recente = df.tail(100).copy()
            print(f"[{datetime.now()}] ⚠️ Usando últimos {len(df_recente)} registros disponíveis")
        
        # Categorias criminais
        cols_roubos_pedestres = ['roubo_transeunte', 'roubo_celular', 'furto_transeunte', 'furto_celular']
        cols_roubos_veiculos = ['roubo_veiculo', 'furto_veiculo']
        cols_violentos = ['hom_doloso', 'tentat_hom', 'lesao_corp_dolosa', 'latrocinio']
        
        # Verificar e converter colunas
        for col_list in [cols_roubos_pedestres, cols_roubos_veiculos, cols_violentos]:
            for col in col_list:
                if col in df_recente.columns:
                    df_recente[col] = pd.to_numeric(df_recente[col], errors='coerce').fillna(0)
                else:
                    if col not in [c for c in col_list if c in df_recente.columns]:
                        print(f"[{datetime.now()}] ⚠️ Coluna não encontrada: {col}, criando com zeros")
                        df_recente[col] = 0
        
        # Agrupar por CISP
        heatmap_data = []
        
        if 'cisp' in df_recente.columns:
            for cisp, group in df_recente.groupby('cisp'):
                try:
                    cod_dp = str(int(float(cisp))).zfill(3) if pd.notna(cisp) else "000"
                    
                    # Buscar coordenadas
                    if cod_dp in geo_mapping:
                        coords = geo_mapping[cod_dp]
                        bairro = coords.get('bairro', f"CISP {cod_dp}")
                    else:
                        # Coordenada padrão (Centro do Rio)
                        coords = {"lat": -22.9068, "lng": -43.1729, "bairro": f"Região {cod_dp}"}
                        bairro = coords['bairro']
                    
                    # Calcular totais
                    total_pedestres = int(group[cols_roubos_pedestres].sum().sum())
                    total_veiculos = int(group[cols_roubos_veiculos].sum().sum())
                    total_letalidade = int(group[cols_violentos].sum().sum())
                    
                    heatmap_data.append({
                        "lat": coords["lat"],
                        "lng": coords["lng"],
                        "bairro": bairro,
                        "pedestres": total_pedestres,
                        "veiculos": total_veiculos,
                        "letalidade": total_letalidade,
                        "total": total_pedestres + total_veiculos + total_letalidade,
                        "cisp": cod_dp
                    })
                except Exception as e:
                    print(f"[{datetime.now()}] ⚠️ Erro processando CISP {cisp}: {e}")
                    continue
        else:
            print(f"[{datetime.now()}] ❌ Coluna 'cisp' não encontrada no dataset")
            # Criar dados de exemplo para teste
            for cod_dp, coords in list(geo_mapping.items())[:20]:
                heatmap_data.append({
                    "lat": coords["lat"],
                    "lng": coords["lng"],
                    "bairro": coords.get('bairro', f"CISP {cod_dp}"),
                    "pedestres": 0,
                    "veiculos": 0,
                    "letalidade": 0,
                    "total": 0,
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
        if heatmap_data:
            total_pedestres = sum(item.get('pedestres', 0) for item in heatmap_data)
            total_veiculos = sum(item.get('veiculos', 0) for item in heatmap_data)
            total_letalidade = sum(item.get('letalidade', 0) for item in heatmap_data)
            
            print(f"\n{'='*50}")
            print(f"📊 RESUMO CRIMINAL")
            print(f"{'='*50}")
            print(f"  👥 Roubos a Pedestres: {total_pedestres:,}")
            print(f"  🚗 Roubos de Veículos: {total_veiculos:,}")
            print(f"  ⚠️  Homicídios/Tentativas: {total_letalidade:,}")
            print(f"  📈 Total Geral: {total_pedestres + total_veiculos + total_letalidade:,}")
            print(f"{'='*50}")
            
            # Top 5 áreas
            top5 = sorted(heatmap_data, key=lambda x: x['total'], reverse=True)[:5]
            print(f"\n🔥 TOP 5 ÁREAS MAIS CRÍTICAS:")
            for i, area in enumerate(top5, 1):
                print(f"  {i}. {area['bairro']}: {area['total']:,} ocorrências")
        
        # Limpar arquivo temporário
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ❌ Erro de conexão: {str(e)}")
        return False
    except Exception as e:
        print(f"[{datetime.now()}] ❌ ERRO CRÍTICO: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = process()
    sys.exit(0 if success else 1)
