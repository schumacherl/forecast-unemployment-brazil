"""
Módulo para coleta e processamento de dados econômicos do Brasil
Fontes: IBGE (SIDRA), Banco Central, IPEADATA
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Classe para carregar dados econômicos brasileiros"""
    
    def __init__(self, data_path: str = "data"):
        self.data_path = Path(data_path)
        self.raw_path = self.data_path / "raw"
        self.processed_path = self.data_path / "processed"
        
        # Criar diretórios se não existirem
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
        # URLs das APIs
        self.ibge_base_url = "https://servicodados.ibge.gov.br/api/v3/agregados"
        self.bcb_base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
        
        # Códigos das séries do IBGE
        self.ibge_codes = {
            'desemprego': '4099',  # Taxa de desemprego - PNAD Contínua
            'ocupacao': '4092',    # Taxa de ocupação
            'participacao': '4093', # Taxa de participação
            'rendimento': '5434'   # Rendimento médio real
        }
        
        # Códigos das séries do Banco Central
        self.bcb_codes = {
            'selic': '432',     # Taxa Selic
            'ipca': '433',      # IPCA
            'pib': '4380',      # PIB mensal
            'cambio': '1'       # Taxa de câmbio
        }

    def get_ibge_data(self, code: str, start_year: int = 2012) -> pd.DataFrame:
        """
        Coleta dados do IBGE via API SIDRA
        
        Args:
            code: Código da série temporal
            start_year: Ano inicial para coleta
            
        Returns:
            DataFrame com os dados coletados
        """
        logger.info(f"Coletando dados IBGE - Código: {code}")
        
        # Construir URL da API
        url = f"{self.ibge_base_url}/{code}/periodos/-36/variaveis/4099"
        params = {
            'localidades': 'N1[all]',  # Brasil
            'classificacao': '',
            'formato': 'json'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Processar dados JSON do IBGE
            records = []
            for item in data[0]['resultados'][0]['series'][0]['serie']:
                date_str = item['periodo']
                value = item['valor']
                
                if value != '...' and value != '-':  # Valores válidos
                    # Converter período (formato YYYYMM)
                    if len(date_str) == 6:
                        year = int(date_str[:4])
                        month = int(date_str[4:])
                        date = pd.Timestamp(year=year, month=month, day=1)
                        
                        records.append({
                            'data': date,
                            'valor': float(value.replace(',', '.'))
                        })
            
            df = pd.DataFrame(records)
            df = df.sort_values('data').reset_index(drop=True)
            
            logger.info(f"Coletados {len(df)} registros do IBGE")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao coletar dados IBGE: {e}")
            return pd.DataFrame()

    def get_bcb_data(self, code: str, start_date: str = "01/01/2012") -> pd.DataFrame:
        """
        Coleta dados do Banco Central via API SGS
        
        Args:
            code: Código da série temporal
            start_date: Data inicial (formato DD/MM/YYYY)
            
        Returns:
            DataFrame com os dados coletados
        """
        logger.info(f"Coletando dados BCB - Código: {code}")
        
        end_date = datetime.now().strftime("%d/%m/%Y")
        url = f"{self.bcb_base_url}/{code}/dados"
        
        params = {
            'formato': 'json',
            'dataInicial': start_date,
            'dataFinal': end_date
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            records = []
            for item in data:
                date_str = item['data']
                value = item['valor']
                
                if value is not None:
                    date = pd.to_datetime(date_str, format='%d/%m/%Y')
                    records.append({
                        'data': date,
                        'valor': float(value)
                    })
            
            df = pd.DataFrame(records)
            df = df.sort_values('data').reset_index(drop=True)
            
            logger.info(f"Coletados {len(df)} registros do BCB")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao coletar dados BCB: {e}")
            return pd.DataFrame()

    def collect_unemployment_data(self) -> pd.DataFrame:
        """Coleta dados específicos de desemprego"""
        logger.info("Iniciando coleta de dados de desemprego...")
        
        # Dados do IBGE - PNAD Contínua
        df_desemprego = self.get_ibge_data(self.ibge_codes['desemprego'])
        
        if not df_desemprego.empty:
            df_desemprego.columns = ['data', 'taxa_desemprego']
            
            # Salvar dados brutos
            filepath = self.raw_path / "taxa_desemprego_ibge.csv"
            df_desemprego.to_csv(filepath, index=False)
            logger.info(f"Dados salvos em: {filepath}")
            
            return df_desemprego
        else:
            logger.error("Falha ao coletar dados de desemprego")
            return pd.DataFrame()

    def collect_economic_indicators(self) -> Dict[str, pd.DataFrame]:
        """
        Coleta todos os indicadores econômicos
        
        Returns:
            Dicionário com DataFrames dos indicadores
        """
        logger.info("Coletando indicadores econômicos...")
        
        indicators = {}
        
        # Dados do Banco Central
        for name, code in self.bcb_codes.items():
            df = self.get_bcb_data(code)
            if not df.empty:
                df.columns = ['data', f'{name}']
                indicators[name] = df
                
                # Salvar dados brutos
                filepath = self.raw_path / f"{name}_bcb.csv"
                df.to_csv(filepath, index=False)
                
            time.sleep(1)  # Rate limiting
        
        # Dados do IBGE
        for name, code in self.ibge_codes.items():
            df = self.get_ibge_data(code)
            if not df.empty:
                df.columns = ['data', f'{name}']
                indicators[name] = df
                
                # Salvar dados brutos
                filepath = self.raw_path / f"{name}_ibge.csv"
                df.to_csv(filepath, index=False)
                
            time.sleep(1)  # Rate limiting
        
        logger.info(f"Coletados {len(indicators)} indicadores")
        return indicators

    def merge_indicators(self, indicators: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Combina todos os indicadores em um DataFrame único
        
        Args:
            indicators: Dicionário com os DataFrames dos indicadores
            
        Returns:
            DataFrame combinado com todos os indicadores
        """
        logger.info("Combinando indicadores...")
        
        if not indicators:
            return pd.DataFrame()
        
        # Começar com o primeiro indicador
        merged_df = None
        
        for name, df in indicators.items():
            if df.empty:
                continue
                
            # Garantir que data seja datetime
            df['data'] = pd.to_datetime(df['data'])
            
            # Converter para frequência mensal (último dia do mês)
            df_monthly = df.copy()
            df_monthly['data'] = df_monthly['data'].dt.to_period('M').dt.end_time
            df_monthly = df_monthly.groupby('data').last().reset_index()
            
            if merged_df is None:
                merged_df = df_monthly
            else:
                merged_df = pd.merge(merged_df, df_monthly, on='data', how='outer')
        
        # Ordenar por data
        merged_df = merged_df.sort_values('data').reset_index(drop=True)
        
        # Salvar dados combinados
        filepath = self.processed_path / "indicadores_economicos.csv"
        merged_df.to_csv(filepath, index=False)
        logger.info(f"Dados combinados salvos em: {filepath}")
        
        return merged_df

    def load_processed_data(self) -> pd.DataFrame:
        """Carrega dados já processados"""
        filepath = self.processed_path / "indicadores_economicos.csv"
        
        if filepath.exists():
            logger.info(f"Carregando dados processados de: {filepath}")
            df = pd.read_csv(filepath)
            df['data'] = pd.to_datetime(df['data'])
            return df
        else:
            logger.warning("Dados processados não encontrados")
            return pd.DataFrame()

    def update_data(self) -> pd.DataFrame:
        """
        Atualiza toda a base de dados
        
        Returns:
            DataFrame com dados atualizados
        """
        logger.info("=== INICIANDO ATUALIZAÇÃO DOS DADOS ===")
        
        # Coletar todos os indicadores
        indicators = self.collect_economic_indicators()
        
        # Combinar indicadores
        df_combined = self.merge_indicators(indicators)
        
        # Relatório da coleta
        if not df_combined.empty:
            logger.info("=== RELATÓRIO DA COLETA ===")
            logger.info(f"Período: {df_combined['data'].min()} a {df_combined['data'].max()}")
            logger.info(f"Total de registros: {len(df_combined)}")
            logger.info(f"Indicadores coletados: {df_combined.columns.tolist()}")
            logger.info("=== COLETA FINALIZADA ===")
        
        return df_combined

    def get_data_summary(self) -> Dict:
        """Retorna resumo dos dados disponíveis"""
        df = self.load_processed_data()
        
        if df.empty:
            return {"status": "Nenhum dado encontrado"}
        
        summary = {
            "total_records": len(df),
            "date_range": {
                "start": df['data'].min().strftime('%Y-%m-%d'),
                "end": df['data'].max().strftime('%Y-%m-%d')
            },
            "indicators": df.columns.tolist(),
            "missing_data": df.isnull().sum().to_dict(),
            "last_update": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return summary


def main():
    """Função principal para teste"""
    loader = DataLoader()
    
    # Atualizar dados
    df = loader.update_data()
    
    # Mostrar resumo
    summary = loader.get_data_summary()
    print("\n=== RESUMO DOS DADOS ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    
    # Mostrar primeiras linhas
    if not df.empty:
        print("\n=== PRIMEIRAS LINHAS ===")
        print(df.head())


if __name__ == "__main__":
    main()