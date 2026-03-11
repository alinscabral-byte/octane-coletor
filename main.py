import requests
import os
import time
import schedule
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURAÇÕES — preencher com seus dados
# ─────────────────────────────────────────────
SEFAZ_TOKEN     = os.environ.get("SEFAZ_TOKEN", "e1603a4aa91c4ac6c8c891d2b58c78b8ef785f1e")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "https://wnieuivunvtnldkjfkxy.supabase.co")
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "")  # service_role key do Supabase

# URL da API da Sefaz AL
SEFAZ_URL = http://api.sefaz.al.gov.br/sfz-economiza-alagoas-api/api/public/
# Combustíveis monitorados
COMBUSTIVEIS = {
    1: "gasolina_comum",
    2: "gasolina_aditivada",
    3: "etanol",
    4: "diesel_comum",
    5: "diesel_aditivado",
}

# Municípios monitorados (código IBGE : nome)
MUNICIPIOS = {
    "2704302": "Maceio",
    "2702207": "Coqueiro Seco",
    "2700300": "Arapiraca",
    "2702306": "Coruripe",
    "2706901": "Pilar",
    "2707701": "Rio Largo",
    "2704708": "Marechal Deodoro",
    "2706307": "Palmeira dos Indios",
    "2708006": "Santana do Ipanema",
    "2706703": "Penedo",
}

# ─────────────────────────────────────────────
# FUNÇÕES PRINCIPAIS
# ─────────────────────────────────────────────

def buscar_precos_sefaz(tipo_combustivel, codigo_ibge):
    """Busca preços de combustível na API da Sefaz AL"""
    headers = {
        "AppToken": SEFAZ_TOKEN,
        "Content-Type": "application/json"
    }
    body = {
        "produto": {
            "tipoCombustivel": tipo_combustivel
        },
        "estabelecimento": {
            "municipio": {
                "codigoIBGE": codigo_ibge
            }
        },
        "dias": 10,
        "pagina": 1,
        "registrosPorPagina": 500
    }

    try:
        resposta = requests.post(SEFAZ_URL, json=body, headers=headers, timeout=30)
        resposta.raise_for_status()
        dados = resposta.json()
        registros = dados.get("conteudo", [])
        print(f"  ✓ {len(registros)} postos encontrados — {MUNICIPIOS.get(codigo_ibge, codigo_ibge)}")
        return registros
    except Exception as erro:
        print(f"  ✗ Erro ao buscar Sefaz: {erro}")
        return []


def salvar_posto_supabase(posto_dados):
    """Salva ou atualiza o posto na tabela 'postos' do Supabase"""
    est = posto_dados.get("estabelecimento", {})
    end = est.get("endereco", {})

    nome = est.get("nomeFantasia") or est.get("razaoSocial", "")
    endereco = f"{end.get('nomeLogradouro', '')}, {end.get('numeroImovel', '')}, {end.get('bairro', '')}".strip(", ")

    dado = {
        "cnpj":      est.get("cnpj", ""),
        "nome":      nome,
        "cidade":    end.get("municipio", ""),
        "estado":    "AL",
        "endereco":  endereco,
        "latitude":  end.get("latitude"),
        "longitude": end.get("longitude"),
        "ativo":     True
    }

    url = f"{SUPABASE_URL}/rest/v1/postos"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # upsert pelo cnpj
    }

    try:
        resposta = requests.post(url, json=dado, headers=headers, timeout=15)
        resposta.raise_for_status()

        # Buscar o ID do posto recém inserido/atualizado
        url_busca = f"{SUPABASE_URL}/rest/v1/postos?cnpj=eq.{dado['cnpj']}&select=id"
        r = requests.get(url_busca, headers=headers, timeout=15)
        resultado = r.json()
        if resultado:
            return resultado[0]["id"]
    except Exception as erro:
        print(f"    ✗ Erro ao salvar posto {nome}: {erro}")

    return None


def salvar_preco_supabase(posto_id, nome_combustivel, preco_dados):
    """Salva o preço na tabela 'precos' do Supabase"""
    venda = preco_dados.get("produto", {}).get("venda", {})

    dado = {
        "posto_id":    posto_id,
        "combustivel": nome_combustivel,
        "preco":       venda.get("valorVenda"),
        "fonte":       "sefaz_al",
        "data_venda":  venda.get("dataVenda"),
    }

    url = f"{SUPABASE_URL}/rest/v1/precos"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    try:
        resposta = requests.post(url, json=dado, headers=headers, timeout=15)
        resposta.raise_for_status()
        return True
    except Exception as erro:
        print(f"    ✗ Erro ao salvar preço: {erro}")
        return False


def executar_coleta():
    """Executa a coleta completa de todos os combustíveis e municípios"""
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"\n{'='*50}")
    print(f"OCTANE — Iniciando coleta: {agora}")
    print(f"{'='*50}")

    total_precos = 0
    total_erros  = 0

    for codigo_ibge, nome_municipio in MUNICIPIOS.items():
        print(f"\n📍 Município: {nome_municipio}")

        for tipo_cod, tipo_nome in COMBUSTIVEIS.items():
            print(f"  ⛽ {tipo_nome.replace('_', ' ').title()}...")

            registros = buscar_precos_sefaz(tipo_cod, codigo_ibge)

            for registro in registros:
                # 1. Salvar o posto
                posto_id = salvar_posto_supabase(registro)

                if posto_id:
                    # 2. Salvar o preço
                    ok = salvar_preco_supabase(posto_id, tipo_nome, registro)
                    if ok:
                        total_precos += 1
                    else:
                        total_erros += 1
                else:
                    total_erros += 1

            # Pequena pausa para não sobrecarregar a API da Sefaz
            time.sleep(1)

    print(f"\n{'='*50}")
    print(f"✅ Coleta finalizada!")
    print(f"   Preços salvos:  {total_precos}")
    print(f"   Erros:          {total_erros}")
    print(f"{'='*50}\n")


# ─────────────────────────────────────────────
# AGENDAMENTO — roda a cada 2 horas
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 OCTANE — Agente de Coleta Sefaz AL iniciado")
    print(f"   Supabase: {SUPABASE_URL}")
    print(f"   Municípios monitorados: {len(MUNICIPIOS)}")
    print(f"   Combustíveis monitorados: {len(COMBUSTIVEIS)}")

    # Roda imediatamente na primeira vez
    executar_coleta()

    # Agenda para rodar a cada 2 horas
    schedule.every(2).hours.do(executar_coleta)

    print("⏰ Agendamento ativo — próxima coleta em 2 horas")
    print("   (Pressione Ctrl+C para parar)\n")

    while True:
        schedule.run_pending()
        time.sleep(60)
