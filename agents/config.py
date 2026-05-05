"""
Configurações e constantes compartilhadas pelos agentes.

Contém:
- COLUMN_MAP: mapeamento dos cabeçalhos do Excel para nomes internos padronizados.
- TANK_CAPACITY: capacidade dos tanques por modelo de veículo (em litros).
- MODEL_CANONICAL_DISPLAY: nome de exibição canônico de cada modelo.
- BRAND_CANONICAL_MAP: nome de exibição canônico de cada marca.
- THRESHOLDS: limiares configuráveis para as regras de auditoria.
- GRAVIDADE_ORDEM: dicionário para ordenar gravidade (BAIXA < MEDIA < ALTA).
- normalizar_texto(): remove acentos e coloca em minúsculas.
- canonicalizar_modelo(): padroniza o nome do modelo do veículo.
- canonicalizar_marca(): padroniza o nome da marca do veículo.
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
    'doblo':    60,
}

# Canonizacao de modelos para reduzir variacoes de escrita na base.
# Chaves em minúsculas sem acentos. Ordem importa: padroes mais especificos
# (multi-palavra) devem vir antes de seus substrings.
MODEL_CANONICAL_DISPLAY = {
    # ── Padrões multi-palavra (antes dos substrings) ────────────────────────
    'chery qq':      'Chery QQ',
    'city class':    'City Class',
    'partida fria':  'Partida Fria',
    't cross':       'T-Cross',
    'cb 500':        'CB 500X',
    'vm 290':        'VM 290',
    'gl 2.5':        'GL 2.5',
    '208 active':    'Peugeot 208',
    'm niks':        'Ambulância',
    '416 cdi':       'Sprinter 416',
    '11.180 drc':    'Delivery 11.180',
    '9.170 drc':     'Delivery 9.170',
    '17.210':        'Constellation 17.210',
    '18.260 crm':    'Constellation 18.260',
    '32.360':        'Constellation 32.360',
    '1039':          'Cargo 1039',
    'robust':        'Robust 18.260',
    # ── Ford ────────────────────────────────────────────────────────────────
    'cargo':         'Cargo',    # antes de 'argo'!
    'fiesta':        'Fiesta',
    'f12000':        'F-12000',
    # ── Fiat ────────────────────────────────────────────────────────────────
    'ducato':        'Ducato',
    'strada':        'Strada',
    'toro':          'Toro',
    'cronos':        'Cronos',
    'doblo':         'Doblo',
    'palio':         'Palio',
    'argo':          'Argo',
    'mobi':          'Mobi',
    'uno':           'Uno',
    'trailblazer':   'Trailblazer',
    'montana':       'Montana',
    'meriva':        'Meriva',
    'celta':         'Celta',
    'onix':          'Onix',
    'spin':          'Spin',
    's10':           'S10',
    # ── Chevrolet ───────────────────────────────────────────────────────────
    'nxr':           'NXR 150',
    'xre':           'XRE 300',
    # ── Hyundai ─────────────────────────────────────────────────────────────
    'hb20':          'HB20',
    # ── Jeep ────────────────────────────────────────────────────────────────
    'renegade':      'Renegade',
    # ── Kia ─────────────────────────────────────────────────────────────────
    'k25000':        'K2500',
    'uk2500':        'UK2500',
    # ── Mercedes-Benz ───────────────────────────────────────────────────────
    'accelo':        'Accelo',
    'atego':         'Atego',
    'sprinter':      'Sprinter',
    # ── Honda ───────────────────────────────────────────────────────────────
    'frontier':      'Frontier',
    # ── Peugeot ─────────────────────────────────────────────────────────────
    'boxer':         'Boxer',
    'partner':       'Partner',
    # ── Renault ─────────────────────────────────────────────────────────────
    'master':        'Master',
    'duster':        'Duster',
    'kwid':          'Kwid',
    'sandero':       'Sandero',
    'logan':         'Logan',
    # ── Toyota ──────────────────────────────────────────────────────────────
    'bandeirante':   'Bandeirante',
    'corolla':       'Corolla',
    'corola':        'Corolla',   # grafia errada na fonte
    # ── Volkswagen ──────────────────────────────────────────────────────────
    'constellation': 'Constellation',
    'microonibus':   'Micro-Ônibus',  # antes de 'onibus'
    'virtus':        'Virtus',
    'voyage':        'Voyage',
    'santana':       'Santana',
    'saveiro':       'Saveiro',
    'delivery':      'Delivery',
    'express':       'Express',
    'volare':        'Volare',    # também cobre 'MARCOPOLO VOLARE'
    'kombi':         'Kombi',
    'polo':          'Polo',
    'gol':           'Gol',
    # ── Volvo ───────────────────────────────────────────────────────────────
    # ── Iveco ───────────────────────────────────────────────────────────────
    'daily':         'Daily',
    'tector':        'Tector',
    'masca':         'Gran Micro',
    # ── Chery ───────────────────────────────────────────────────────────────
    'tigo':          'Tigo',
    # ── Equipamentos / genéricos ────────────────────────────────────────────
    'trator':        'Trator',
    'gerador':       'Gerador',
    'rocadeira':     'Roçadeira',
    'maquina':       'Máquina',
    'onibus':        'Ônibus',
    'caminhao':      'Caminhão',
    # ── Padrões curtos (por último para evitar falsos positivos) ────────────
    'van':           'Van',
    'ka':            'Ka',
}

# ---------------------------------------------------------------------------
# Canonizacao de marcas para consolidar variações de grafia na base.
# Chaves: texto normalizado (sem acentos, minúsculas).
# ---------------------------------------------------------------------------
BRAND_CANONICAL_MAP = {
    'caoa chery':   'Chery',
    'chery':        'Chery',
    'chevrolet':    'Chevrolet',
    'fiat':         'Fiat',
    'ford':         'Ford',
    'foton':        'Foton',
    'honda':        'Honda',
    'hyundai':      'Hyundai',
    'i/ kia':       'Kia',
    'iveco - fiat': 'Iveco',
    'iveco':        'Iveco',
    'jeep':         'Jeep',
    'kia':          'Kia',
    'marcopolo':    'Marcopolo',
    'mercedes benz':  'Mercedes-Benz',
    'mercedes-benz':  'Mercedes-Benz',
    'mitsubishi':   'Mitsubishi',
    'nissan':       'Nissan',
    'outros':       'Outros',
    'peugeot':      'Peugeot',
    'randon':       'Randon',
    'renault':      'Renault',
    'stihl':        'Stihl',
    'toyota':       'Toyota',
    'volkswagen':   'Volkswagen',
    'volvo':        'Volvo',
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
    'tolerancia_valor_total_pct':     0.02,  # 2% de diferença tolerada em valor total (era fixo R$0,10)
    'tolerancia_valor_total_abs':     0.50,  # piso absoluto: diferenças menores que R$0,50 ignoradas
    'tolerancia_preco_unitario':      0.05,  # R$/L tolerado no preço unitário
    'minimo_historico_para_comparacao': 5,   # mín de registros históricos para R09/R10 (5 = ~1 mês se semanal)
    'dias_historico_rolling':                90,  # janela primária em dias para calcular médias (R03/R09/R10)
    'dias_historico_rolling_fallback':       180, # janela de fallback por placa quando 90 dias tem < mínimo
    'usar_mad_outlier':               True,  # True = MAD robusto; False = mean±σ clássico
    'fator_mad_historico':            3.0,   # fator k para MAD: mediana ± k*1.4826*MAD (R09)
    'fator_desvio_historico':         2.0,   # fator σ para fallback mean±σ (R09 sem MAD)
    'razao_km_rodado_esperado_min':     60.0,  # R10 dispara quando km_rodado < X% do km_esperado
    # ── Validação de quilometragem e consumo ──────────────────────────────
    'km_rodado_max_valido':        2000,   # km entre abastecimentos acima disso = dado inválido
    'km_atual_max_valido':       999_999,  # leitura de odômetro acima disso = impossível
    'consumo_max_valido':           30.0,  # km/L acima disso = hodômetro inválido (nenhum veículo terrestre supera isso)
    'consumo_min_valido':             2.0,  # km/L abaixo disso = km zerado ou volume absurdo
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


def canonicalizar_marca(valor) -> str:
    """Converte variações de marca para um nome canônico amigável.

    Consolida duplicidades como 'MERCEDES BENZ' / 'MERCEDES-BENZ',
    'IVECO' / 'IVECO - FIAT', 'CAOA CHERY' / 'CHERY', etc.
    """
    bruto = '' if valor is None else str(valor).strip()
    if not bruto or bruto.lower() in ('nan', 'none', ''):
        return ''

    texto_norm = normalizar_texto(bruto)
    if texto_norm in BRAND_CANONICAL_MAP:
        return BRAND_CANONICAL_MAP[texto_norm]
    # Fallback: Title Case do valor original
    return bruto.title()
