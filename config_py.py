"""
Configurações do projeto
"""

import os
from pathlib import Path

# Caminhos do projeto
PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"
MODELS_PATH = PROJECT_ROOT / "models"
REPORTS_PATH = PROJECT_ROOT / "reports"

# Configurações da API
API_CONFIG = {
    'timeout': 30,
    'max_retries': 3,
    'rate_limit_delay': 1,  # segundos entre requests
}

# Códigos das séries temporais
IBGE_SERIES = {
    'desemprego': {
        'code': '4099',
        'name': 'Taxa de desemprego',
        'unit': '%',
        'source': 'PNAD Contínua'
    },
    'ocupacao': {
        'code': '4092',
        'name': 'Taxa de ocupação',
        'unit': '%',
        'source': 'PNAD Contínua'
    },
    'participacao': {
        'code': '4093',
        'name': 'Taxa de participação',
        'unit': '%',
        'source': 'PNAD Contínua'
    },
    'rendimento': {
        'code': '5434',
        'name': 'Rendimento médio real',
        'unit': 'R$',
        'source': 'PNAD Contínua'
    }
}

BCB_SERIES = {
    'selic': {
        'code': '432',
        'name': 'Taxa Selic',
        'unit': '% a.a.',
        'source': 'BCB-SGS'
    },
    'ipca': {
        'code': '433',
        'name': 'IPCA',
        'unit': '% a.m.',
        'source': 'BCB-SGS'
    },
    'pib': {
        'code': '4380',
        'name': 'PIB mensal',
        'unit': 'Índice',
        'source': 'BCB-SGS'
    },
    'cambio': {
        'code': '1',
        'name': 'Taxa de câmbio',
        'unit': 'R$/US$',
        'source': 'BCB-SGS'
    },
    'igpm': {
        'code': '189',
        'name': 'IGP-M',
        'unit': '% a.m.',
        'source': 'BCB-SGS'
    }
}

# Configurações dos modelos
MODEL_CONFIG = {
    'train_test_split': 0.8,
    'validation_split': 0.2,
    'random_state': 42,
    'cross_validation_folds': 5,
}

# Configurações de visualização
PLOT_CONFIG = {
    'figsize': (12, 8),
    'dpi': 300,
    'style': 'whitegrid',
    'color_palette': 'husl',
    'save_format': 'png'
}

# Configurações de logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['console', 'file']
}