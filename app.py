import os
import json
import requests
import base64
import pandas as pd
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. CONFIGURAÇÕES DE AMBIENTE (LENDO DO GITHUB SECRETS) ---

def get_sheet():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # O GitHub lê o JSON da conta de serviço direto dos Secrets
        creds_json = os.getenv("GOOGLE_SHEETS_JSON")
        if not creds_json:
            print("Erro: Variável GOOGLE_SHEETS_JSON não encontrada.")
            return None
            
        creds_info = json.loads(creds_json)
        creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        client = gspread.authorize(creds)
        
        # Abre a planilha pela URL
        return client.open_by_url("https://docs.google.com/spreadsheets/d/10vGoOF-_qGTrmoCrUipQC3pmSXkL8QeUk7AI0tVWjao/edit#gid=0")
    except Exception as e:
        print(f"Erro na conexão com Google Sheets: {e}")
        return None

def obter_token(empresa_nome, spreadsheet):
    sh = spreadsheet.sheet1 # Assume que os tokens estão na primeira aba
    try:
        cell = sh.find(empresa_nome)
        rt = sh.cell(cell.row, 2).value
        
        client_id = os.getenv("CONTA_AZUL_CLIENT_ID")
        client_secret = os.getenv("CONTA_AZUL_CLIENT_SECRET")
        
        auth_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        res = requests.post("https://auth.contaazul.com/oauth2/token", 
            headers={"Authorization": f"Basic {auth_b64}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": rt})
        
        if res.status_code == 200:
            dados = res.json()
            if dados.get('refresh_token'): 
                sh.update_cell(cell.row, 2, dados['refresh_token'])
            return dados['access_token']
    except Exception as e:
        print(f"Erro ao obter token para {empresa_nome}: {e}")
    return None

def buscar_v2(endpoint, token, params):
    itens_acumulados = []
    headers = {"Authorization": f"Bearer {token}"}
    params.update({"status": "EM_ABERTO", "tamanho_pagina": 100, "pagina": 1})
    while True:
        res = requests.get(f"https://api-v2.contaazul.com{endpoint}", headers=headers, params=params)
        if res.status_code != 200: break
        itens = res.json().get('itens', [])
        if not itens: break
        for i in itens:
            saldo = i.get('total', 0) - i.get('pago', 0)
            if saldo > 0:
                itens_acumulados.append({"Vencimento": i.get("data_vencimento"), "Valor": saldo})
        if len(itens) < 100: break
        params["pagina"] += 1
    return itens_acumulados

# --- 2. EXECUÇÃO DO PROCESSO ---

ss = get_sheet()
if ss:
    hoje = datetime.now().date()
    data_ini = (hoje - timedelta(days=30)).isoformat()
    data_fim = (hoje + timedelta(days=90)).isoformat() # Aumentado para 90 dias para melhor visão no Looker
    
    # Puxa clientes da Coluna A da aba principal
    clientes = [r[0] for r in ss.sheet1.get_all_values()[1:] if r[0]]
    p_total, r_total = [], []

    for emp in clientes:
        print(f"Sincronizando: {emp}...")
        tk = obter_token(emp, ss)
        if tk:
            api_params = {"data_vencimento_de": data_ini, "data_vencimento_ate": data_fim}
            
            pagar = buscar_v2("/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar", tk, api_params.copy())
            receber = buscar_v2("/v1/financeiro/eventos-financeiros/contas-a-receber/buscar", tk, api_params.copy())
            
            for i in pagar: i.update({"Empresa": emp, "Tipo": "Despesa"})
            for i in receber: i.update({"Empresa": emp, "Tipo": "Receita"})
            
            p_total.extend(pagar)
            r_total.extend(receber)

   # --- 3. ATUALIZAÇÃO DA BASE DO LOOKER ---
    if p_total or r_total:
        df_finais = pd.DataFrame(p_total + r_total)
        
        # Garante que Valor seja numérico para o Looker somar corretamente
        df_finais['Valor'] = pd.to_numeric(df_finais['Valor'], errors='coerce').fillna(0)
        
        # Selecionamos as colunas na ordem correta
        df_finais = df_finais[['Vencimento', 'Empresa', 'Tipo', 'Valor']]
        
        try:
            # Tenta encontrar ou criar a aba Base_Looker
            try:
                worksheet = ss.worksheet("Base_Looker")
            except Exception:
                worksheet = ss.add_worksheet(title="Base_Looker", rows="5000", cols="5")
            
            # Limpa e atualiza os dados
            worksheet.clear()
            
            cabecalho = [df_finais.columns.values.tolist()]
            corpo_dados = df_finais.values.tolist()
            exportar = cabecalho + corpo_dados
            
            worksheet.update(exportar)
            print("Sincronização Finalizada! Dados enviados como números para o Looker.")
            
        except Exception as e:
            print(f"Erro ao salvar na planilha: {e}")
    else:
        print("Nenhum dado encontrado para sincronizar.")
