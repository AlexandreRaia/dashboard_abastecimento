"""
Configurações e constantes compartilhadas pelos agentes.

Contém:
- COLUMN_MAP: mapeamento dos cabeçalhos do Excel para nomes internos padronizados.
- TANK_CAPACITY: capacidade dos tanques por modelo de veículo (em litros).
- MODEL_CANONICAL_DISPLAY: nome de exibição canônico de cada modelo.
- THRESHOLDS: limiares configuráveis para as regras de auditoria.
- GRAVIDADE_ORDEM: dicionário para ordenar gravidade (BAIXA < MEDIA < ALTA).
- normalizar_texto(): remove acentos e coloca em minúsculas.
- canonicalizar_modelo(): padroniza o nome do modelo do veículo.
"""
import unicodedata

# ---------------------------------------------------------------------------
# Mapeamento de colunas do Excel para nomes internos
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    'Data/Hora':       'data_hora',
    'Placa':           'placa',
    'Condutor':        'condutor',
    'Modelo':          'modelo',
    'Marca':           'marca',
    'Ult. km':         'ult_km',
    'km Atual':        'km_atual',
    'km/L':            'km_l_informado',
    'Km Rodado':       'km_rodado',
    'KM Minimo':       'km_minimo',
    'KM Maximo':       'km_maximo',
    'Qtde (L)':        'litros',
    'Vr. Unit.':       'valor_unitario',
    'Valor':           'valor_total',
    'Estabelecimento': 'estabelecimento',
    'Produto':         'produto',
    'Unidade':         'unidade',
    'Tipo Frota':      'tipo_frota',
}

# ---------------------------------------------------------------------------
# Capacidade dos tanques por modelo (litros)
# Chaves em minúsculas e sem acentos para comparacao normalizada
# ---------------------------------------------------------------------------
TANK_CAPACITY = {
    'mobi':     47,
    'uno':      48,
    'ka':       48,
    'onix':     50,
    'spin':     53,
    'sandero':  50,
    'logan':    50,
    'saveiro':  54,
    'strada':   54,
    'van':     105,
    'master':  105,
    'sprinter': 100,
    'caminhao': 120,
    'kombi':    55,
    'ducato':   95,
}

# Canonizacao de modelos para reduzir variacoes de escrita na base.
MODEL_CANONICAL_DISPLAY = {
    'mobi': 'Mobi',
    'uno': 'Uno',
    'ka': 'Ka',
    'onix': 'Onix',
    'spin': 'Spin',
    'sandero': 'Sandero',
    'logan': 'Logan',
    'saveiro': 'Saveiro',
    'strada': 'Strada',
    'van': 'Van',
    'master': 'Master',
    'sprinter': 'Sprinter',
    'caminhao': 'Caminhao',
    'kombi': 'Kombi',
    'ducato': 'Ducato',
}

# ---------------------------------------------------------------------------
# Limiares configuráveis — ajuste sem alterar a lógica dos agentes
# ---------------------------------------------------------------------------
THRESHOLDS = {
    'fator_tolerancia_tanque':        1.00,  # litros >= capacidade * fator -> ALTA
    'fator_consumo_critico':          0.50,  # fallback legado: consumo < km_min * fator -> ALTA
    'fator_outlier_consumo_critico':  2.0,   # novo R03: consumo < media_modelo - N*desvio_modelo -> ALTA
    'min_amostra_outlier_consumo':    5,     # minimo de registros por modelo para aplicar outlier no R03
    'limite_intervalo_horas':         4,     # intervalo < N h entre abastecimentos -> MEDIA
    'km_baixo_limite':                30,    # km rodado abaixo deste para...
    'litros_alto_limite':             40,    # ...litros acima deste -> ALTA
    'tolerancia_valor_total':         0.10,  # R$ diferença tolerada em valor total
    'tolerancia_preco_unitario':      0.05,  # R$/L tolerado no preço unitário
    'minimo_historico_para_comparacao': 5,   # mín de registros históricos para R08/R09
    'fator_desvio_historico':         2.0,   # media + N*desvio para R08
    'desvio_km_esperado_pct':         40.0,  # % abaixo do km esperado para R09
}

# Ordem crescente de gravidade para comparações
GRAVIDADE_ORDEM = {'BAIXA': 1, 'MEDIA': 2, 'ALTA': 3}


def normalizar_texto(valor) -> str:
    """Remove acentos e converte para minúsculas."""
    if not valor or str(valor).strip().lower() in ('nan', 'none', ''):
        return ''
    texto = str(valor).strip().lower()
    return ''.join(
        ch for ch in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(ch)
    )


def canonicalizar_modelo(valor) -> str:
    """Converte variacoes de modelo para um nome canônico amigável."""
    bruto = '' if valor is None else str(valor).strip()
    if not bruto or bruto.lower() in ('nan', 'none', ''):
        return ''

    texto_norm = normalizar_texto(bruto)
    for chave, nome in MODEL_CANONICAL_DISPLAY.items():
        if chave in texto_norm:
            return nome
    return bruto
