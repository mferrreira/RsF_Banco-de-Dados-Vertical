"""
SAFELIVING — SIMULADOR EDGE / FOG / CLOUD (cenário de reconhecimento veicular)
================================================================================
A SafeLiving administra ecossistemas residenciais inteligentes. Cada casa tem
câmera de garagem, sensor de presença e medidor de energia. Este simulador
reencena a chegada de um veículo na Casa #104 para mostrar, em código, as
regras de TRANSMISSÃO, PROCESSAMENTO e ABSTRAÇÃO em cada camada:

    EDGE  -> Gateway residencial (dentro de cada casa)
             Faz a FUSÃO câmera + sensor de presença e age sozinho, em
             milissegundos: abre o portão e liga os refletores, mesmo sem
             conexão externa.

    FOG   -> Servidor de borda na antena OpenRAN de cada bairro
             Correlaciona o acesso com o sensor de trânsito público do
             quarteirão. Se confirmado, aplica Network Slicing e aciona a
             iluminação pública ao longo do trajeto do morador.

    CLOUD -> Aplicação de Gestão de Propriedades e Segurança Global
             Dispara notificação push, ajusta a climatização, agenda a
             recarga do veículo elétrico na tarifa reduzida, e usa as
             imagens de placa para re-treinar o modelo de visão
             computacional — distribuindo a atualização de volta para
             os gateways Edge e os nós de Fog.

Como usar em sala:
    python safeliving_edge_fog_cloud.py

Ao final, o script:
  1) mostra o passo a passo no terminal;
  2) GERA E ABRE UM DASHBOARD EM PÁGINA WEB SEPARADA
     (arquivo dashboard_safeliving.html, aberto automaticamente no navegador);
  3) abre um menu no terminal para você explorar os dados coletados.
"""

import random
import time
import os
import webbrowser
import sqlite3
from datetime import datetime
from collections import Counter


# ======================================================================
# CONFIGURAÇÕES GERAIS (o que os alunos vão mexer primeiro)
# ======================================================================
CICLOS_DE_SIMULACAO = 8
LIMIAR_RETREINO_VISAO = 3   # nº de acessos validados para a Cloud re-treinar o modelo

LATENCIA_CASA_PARA_5G_SEG = 0.02
LATENCIA_RAN_PARA_CLOUD_SEG = 0.15

# Chegadas de veículo roteirizadas, para reencenar o cenário de forma
# reprodutível: (casa_numero, bairro_id, ciclo, placa)
SCRIPT_CHEGADAS = [
    (104, "bloco-A", 2, "ABC1D23"),
    (207, "bloco-A", 4, "XYZ9K88"),
    (104, "bloco-A", 6, "ABC1D23"),   # o morador da 104 chega uma 2ª vez
    (150, "bloco-B", 5, "QRT4L56"),
    (88,  "bloco-A", 7, "HHH0099"),   # esta NÃO terá confirmação do trânsito público
]

# O sensor de trânsito público do quarteirão confirma a maioria das chegadas,
# menos a da Casa #88 — de propósito, para os alunos verem a Fog tratando
# um acesso "não confirmado regionalmente" de forma diferente.
PLACAS_CONFIRMADAS_TRANSITO_PUBLICO = {
    (bairro, placa, ciclo)
    for (casa, bairro, ciclo, placa) in SCRIPT_CHEGADAS
    if casa != 88
}

NOME_ARQUIVO_DASHBOARD = "dashboard_safeliving.html"


# ======================================================================
# CAMADA 0 — DISPOSITIVOS RESIDENCIAIS (sensores físicos dentro da casa)
# ======================================================================
class DispositivoResidencial:
    """Gera dados brutos. Não processa nada, não decide nada."""

    def __init__(self, id_dispositivo, tipo, casa_numero):
        self.id = id_dispositivo
        self.tipo = tipo
        self.casa_numero = casa_numero

    def gerar_leitura(self, ciclo_atual):
        chegada = next(
            (c for c in SCRIPT_CHEGADAS if c[0] == self.casa_numero and c[2] == ciclo_atual),
            None,
        )

        if self.tipo == "camera_garagem":
            valor = chegada[3] if chegada else None            # placa detectada (ou nenhuma)
        elif self.tipo == "sensor_presenca_garagem":
            if chegada:
                valor = 1
            else:
                valor = 1 if random.random() < 0.03 else 0     # ruído: animal, folha, etc.
        elif self.tipo == "medidor_energia":
            valor = round(random.uniform(0.3, 2.5), 2)
        else:
            valor = None

        return {
            "casa_numero": self.casa_numero,
            "tipo": self.tipo,
            "valor": valor,
            "hora": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        }


# ======================================================================
# CAMADA 1 — EDGE: GATEWAY RESIDENCIAL (dentro da casa)
# ======================================================================
class CasaInteligente:
    """
    TRANSMISSÃO: uplink 5G até a antena do bairro, distância mínima.
    PROCESSAMENTO: funde câmera + sensor de presença e decide sozinha,
                   em milissegundos, sem depender de rede externa.
    ABSTRAÇÃO: envia à Fog só o evento de acesso (casa + placa + hora),
               nunca o vídeo bruto da câmera.
    """
    def __init__(self, casa_numero, bairro_id, dispositivos):
        self.casa_numero = casa_numero
        self.bairro_id = bairro_id
        self.dispositivos = dispositivos
        self.versao_software = 1.0
        # Banco SQLite local da Edge (um por casa)
        self.db_edge = sqlite3.connect(f"db_edge_casa-{casa_numero}.db")
        self._criar_tabelas_edge()

    def _criar_tabelas_edge(self):
        with self.db_edge:
            self.db_edge.execute(
                "CREATE TABLE IF NOT EXISTS acessos (" \
                "id INTEGER PRIMARY KEY AUTOINCREMENT," \
                "placa TEXT," \
                "status TEXT," \
                "hora TEXT)"
            )
            self.db_edge.execute(
                "CREATE TABLE IF NOT EXISTS telemetria (" \
                "id INTEGER PRIMARY KEY AUTOINCREMENT," \
                "valor_kwh REAL," \
                "hora TEXT)"
            )
            self.db_edge.execute(
                "CREATE TABLE IF NOT EXISTS firmware (" \
                "versao REAL)"
            )
            self.db_edge.execute("INSERT OR IGNORE INTO firmware (versao) VALUES (1.0)")



    # ---------------- REGRA DE PROCESSAMENTO NA CASA (fusão câmera + presença) ----------------
    def processar(self, leituras):
        placa = leituras["camera_garagem"]["valor"]
        presenca = leituras["sensor_presenca_garagem"]["valor"]

        if placa and presenca == 1:
            print(f"    [CASA #{self.casa_numero}] AÇÃO AUTÔNOMA (ms): fusão câmera+presença "
                  f"confirma veículo na zona de entrada -> liga refletores e abre o portão.")
            return "ACESSO_VEICULO_AUTORIZADO", placa

        if presenca == 1 and not placa:
            print(f"    [CASA #{self.casa_numero}] presença detectada, mas sem placa "
                  f"confirmada -> portão permanece fechado (fusão evita ação incorreta).")
        return None, None
    # ------------------------------------------------------------------------------------------

    # ---------------- REGRA DE ABSTRAÇÃO NA CASA (EDGE) ----------------
    def abstrair_acesso(self, placa, hora):
        return {
            "tipo_evento": "ACESSO_VEICULO_AUTORIZADO",
            "casa_numero": self.casa_numero,
            "placa": placa,
            "hora": hora,
        }

    def abstrair_telemetria(self, leitura_energia):
        return {
            "tipo_evento": "TELEMETRIA_ENERGIA",
            "casa_numero": self.casa_numero,
            "valor_kwh": leitura_energia["valor"],
            "hora": leitura_energia["hora"],
        }
    # --------------------------------------------------------------------------

    def ciclo(self, ciclo_atual):
        leituras = {disp.tipo: disp.gerar_leitura(ciclo_atual) for disp in self.dispositivos}
        eventos = []

        tipo_evento, placa = self.processar(leituras)

        # ---------------- REGRA DE TRANSMISSÃO NA CASA (EDGE) ----------------
        if tipo_evento == "ACESSO_VEICULO_AUTORIZADO":
            time.sleep(LATENCIA_CASA_PARA_5G_SEG)
            eventos.append(self.abstrair_acesso(placa, leituras["camera_garagem"]["hora"]))
            self._salvar_acesso(placa, leituras["camera_garagem"]["hora"])

        if random.random() < 0.3:  # telemetria de energia é enviada só ocasionalmente
            time.sleep(LATENCIA_CASA_PARA_5G_SEG)
            eventos.append(self.abstrair_telemetria(leituras["medidor_energia"]))
            self._salvar_telemetria(leituras["medidor_energia"])
        # --------------------------------------------------------------------------
        return eventos

    # ---------------- PERSISTÊNCIA LOCAL DA EDGE ----------------
    def _salvar_acesso(self, placa, hora):
        with self.db_edge:
            self.db_edge.execute(
                "INSERT INTO acessos (placa, status, hora) VALUES (?,?,?)",
                (placa, "AUTORIZADO", hora),
            )

    def _salvar_telemetria(self, leitura_energia):
        with self.db_edge:
            self.db_edge.execute(
                "INSERT INTO telemetria (valor_kwh, hora) VALUES (?,?)",
                (leitura_energia["valor"], leitura_energia["hora"]),
            )
    # --------------------------------------------------------------------------

    # ---------------- A CLOUD PODE ATUALIZAR O SOFTWARE DESTE GATEWAY ----------------
    def receber_atualizacao_software(self, nova_versao):
        self.versao_software = nova_versao
        with self.db_edge:
            self.db_edge.execute("UPDATE firmware SET versao = ?", (nova_versao,))
        print(f"    [CASA #{self.casa_numero}] gateway atualizado para a versão "
              f"{nova_versao:.1f} do modelo de visão computacional.")
    # ------------------------------------------------------------------------------------------


# ======================================================================
# CAMADA 2 — FOG: SERVIDOR DE BORDA NA ANTENA OPENRAN DO BAIRRO
# ======================================================================
class EstacaoORAN:
    """
    TRANSMISSÃO: correlaciona em lote antes de escalar à Cloud.
    PROCESSAMENTO: confirma o acesso com o sensor de trânsito público do
                   quarteirão; se confirmado, aplica Network Slicing e
                   aciona a iluminação pública do trajeto.
    ABSTRAÇÃO: entrega à Cloud um evento consolidado do bairro, não os
               pacotes brutos recebidos de cada casa.
    """

    def __init__(self, bairro_id, casas_atendidas):
        self.bairro_id = bairro_id
        self.casas_atendidas = casas_atendidas
        self.versao_software = 1.0
        self.buffer = []

        #Criando banco de dados
        self.db = sqlite3.connect(f"db_fog_bairro-{bairro_id}.db")
        self.criar_tabelas()
        

    def criar_tabelas(self):
        with self.db:
            #Criando a tabela do log de eventos dos carros que entraram
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS eventos (" \
                "id INTEGER PRIMARY KEY AUTOINCREMENT," \
                "casaNumero INTEGER," \
                "placa TEXT," \
                "status TEXT," \
                "hora TEXT" \
                ")"
            )
            #Criando a tabela contendo o status do serviço público (iluminação pública)
            self.db.execute("" \
            "CREATE TABLE IF NOT EXISTS status_servico(" \
            "servico TEXT PRIMARY KEY," \
            "status TEXT" \
            ")")
            #Inicializando O serviço público de luzes como OFF sem repetir
            self.db.execute("INSERT OR IGNORE INTO status_servico (servico, status) VALUES ('iluminacao_publica', 'OFF')")

    def receber(self, eventos_da_casa):
        self.buffer.extend(eventos_da_casa)

    # ---------------- AÇÕES DA FOG QUANDO O ACESSO É CONFIRMADO ----------------
    def aplicar_slicing_rede(self, bairro_id):
        print(f"    [FOG {bairro_id}] Network Slicing acionado: canal de comunicação "
              f"da quadra priorizado na OpenRAN.")

    def acionar_iluminacao_publica(self, bairro_id):
        #Verificando o estado atual da iluminação pública daquele bairro
        cursor_fog = self.db.cursor()
        #Buscando no banco de dados o estado atual da iluminação pública e colocando o cursor na tupla do estado atual
        cursor_fog.execute("SELECT status FROM status_servico WHERE servico = 'iluminacao_publica'")

        #Armazenando o valor do estado que está na primeira (0) posição do cursor_fog
        status_iluminacao_publica = cursor_fog.fetchone()[0]

        #Alterando as luzes à depender do estado atual
        if status_iluminacao_publica == "OFF":
            print(f"    [FOG {bairro_id}] painéis de iluminação pública acionados ao "
              f"longo do trajeto até a garagem.")
            #Alterando o estado atual no banco de dados
            with self.db:
                self.db.execute("UPDATE status_servico SET status = 'ON' WHERE servico = 'iluminacao_publica'")
        else:
            print(f"    [FOG {bairro_id}] Identificou que as luzes já estão ligadas!")

    def db_salvar_evento(self, casa_numero, placa, status, hora):
        with self.db:
            self.db.execute("INSERT INTO eventos (casaNumero, placa, status, hora) VALUES " \
            "(?,?,?,?)", (casa_numero, placa, status, hora))
    # --------------------------------------------------------------------------------

    # ---------------- REGRA DE PROCESSAMENTO NA FOG (xApp / Near-RT RIC) ----------------
    def xapp_processar_lote(self, ciclo_atual):
        acessos_validados = []
        acessos_nao_confirmados = []
        eventos_energia = 0

        for e in self.buffer:
            if e["tipo_evento"] == "ACESSO_VEICULO_AUTORIZADO":
                confirmado = (self.bairro_id, e["placa"], ciclo_atual) in PLACAS_CONFIRMADAS_TRANSITO_PUBLICO
                alvo = acessos_validados if confirmado else acessos_nao_confirmados
                alvo.append({"casa_numero": e["casa_numero"], "placa": e["placa"]})

                #Salvando o evento de acesso no banco de dados
                status_acesso = "CONFIRMADO" if confirmado else "NÃO_CONFIRMADO"
                self.db_salvar_evento(e["casa_numero"], e["placa"], status_acesso, e["hora"])
            elif e["tipo_evento"] == "TELEMETRIA_ENERGIA":
                eventos_energia += 1

        if acessos_validados:
            status = "ACESSO_VALIDADO_REGIONALMENTE"
            self.aplicar_slicing_rede(self.bairro_id)
            self.acionar_iluminacao_publica(self.bairro_id)
        elif acessos_nao_confirmados:
            status = "ACESSO_NAO_CONFIRMADO"
        elif eventos_energia:
            status = "TELEMETRIA_ROTINA"
        else:
            status = "NORMAL"

        return status, acessos_validados, acessos_nao_confirmados, eventos_energia
    # ------------------------------------------------------------------------------------------

    # ---------------- REGRA DE ABSTRAÇÃO NA FOG (Near-RT RIC -> Cloud) ----------------
    def abstrair(self, status, validados, nao_confirmados, eventos_energia):
        return {
            "bairro_id": self.bairro_id,
            "status_bloco": status,
            "acessos_validados": validados,
            "acessos_nao_confirmados": nao_confirmados,
            "eventos_energia": eventos_energia,
            "hora": datetime.now().strftime("%H:%M:%S"),
        }
    # ------------------------------------------------------------------------------------------

    def ciclo(self, ciclo_atual):
        if not self.buffer:
            return None
        time.sleep(LATENCIA_RAN_PARA_CLOUD_SEG)
        status, validados, nao_confirmados, energia = self.xapp_processar_lote(ciclo_atual)
        relatorio = self.abstrair(status, validados, nao_confirmados, energia)
        print(f"  [FOG {self.bairro_id}] lote processado -> status_bloco={status}")
        self.buffer = []
        return relatorio

    # ---------------- A CLOUD PODE ATUALIZAR O SOFTWARE DESTE NÓ DE FOG ----------------
    def receber_atualizacao_software(self, nova_versao):
        self.versao_software = nova_versao
        print(f"    [FOG {self.bairro_id}] nó de borda atualizado para a versão "
              f"{nova_versao:.1f} do modelo de visão computacional.")
    # ------------------------------------------------------------------------------------------


# ======================================================================
# CAMADA 3 — CLOUD: GESTÃO DE PROPRIEDADES E SEGURANÇA GLOBAL
# ======================================================================
class NucleoCentral:
    """
    TRANSMISSÃO: maior distância/latência, recebe eventos já consolidados.
    PROCESSAMENTO: dispara integrações de alto nível (push, climatização,
                   recarga do veículo) e re-treina o modelo de visão.
    ABSTRAÇÃO: não vê o vídeo nem os frames — só o evento de acesso já
               resolvido (casa, placa, horário), o suficiente para
               personalizar o app do morador e alimentar o ML.
    """

    def __init__(self, fog_nodes, casas):
        self.fog_nodes = fog_nodes
        self.casas = casas
        self.historico = []
        self.placas_para_retreino = []
        self.versao_modelo = 1.0
        self.ajustes_modelo = []

    def registrar(self, relatorio_fog):
        self.historico.append(relatorio_fog)
        print(f"  [CLOUD] evento consolidado registrado na base de dados temporal "
              f"(bairro {relatorio_fog['bairro_id']}, status {relatorio_fog['status_bloco']})")

        # ---------------- REGRA DE PROCESSAMENTO NA CLOUD (integrações de alto nível) ----------------
        for acesso in relatorio_fog["acessos_validados"]:
            casa = acesso["casa_numero"]
            print(f"  [CLOUD] notificação push enviada ao smartphone do morador da Casa #{casa}")
            print(f"  [CLOUD] climatização interna ajustada conforme preferências da Casa #{casa}")
            print(f"  [CLOUD] recarga do veículo elétrico da Casa #{casa} agendada para a "
                  f"janela de tarifa reduzida (23h–06h)")

            self.placas_para_retreino.append(acesso)
            if len(self.placas_para_retreino) % LIMIAR_RETREINO_VISAO == 0:
                self._retreinar_modelo_visao()
        # --------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------

    # ---------------- AÇÃO ROBUSTA DA CLOUD: ML + DEPLOY DE VOLTA PARA EDGE E FOG ----------------
    def _retreinar_modelo_visao(self):
        self.versao_modelo = round(self.versao_modelo + 0.1, 1)
        print(f"  [CLOUD] ML: re-treinando modelo de visão computacional com "
              f"{len(self.placas_para_retreino)} placas capturadas...")
        print(f"  [CLOUD] Nova versão do modelo: {self.versao_modelo:.1f}. "
              f"Distribuindo atualização para gateways Edge e nós de Fog.")

        for casa in self.casas:
            casa.receber_atualizacao_software(self.versao_modelo)
        for fog in self.fog_nodes:
            fog.receber_atualizacao_software(self.versao_modelo)

        self.ajustes_modelo.append({
            "hora": datetime.now().strftime("%H:%M:%S"),
            "versao": self.versao_modelo,
            "amostras_usadas": len(self.placas_para_retreino),
            "gateways_atualizados": len(self.casas),
            "nos_fog_atualizados": len(self.fog_nodes),
        })
    # ------------------------------------------------------------------------------------------

    # ======================================================================
    # DASHBOARD DA CLOUD — PÁGINA WEB SEPARADA (não é impresso no terminal)
    # ======================================================================
    def gerar_dashboard_html(self):
        contagem_status = Counter(r["status_bloco"] for r in self.historico)
        max_contagem = max(contagem_status.values(), default=1)
        bairros_monitorados = sorted({r["bairro_id"] for r in self.historico})
        total_validados = sum(len(r["acessos_validados"]) for r in self.historico)
        total_nao_confirmados = sum(len(r["acessos_nao_confirmados"]) for r in self.historico)

        def fmt_acessos(lista):
            return ", ".join(f"#{a['casa_numero']} ({a['placa']})" for a in lista) or "—"

        linhas_timeline = "\n".join(
            f"""<tr>
                  <td class="mono">{r['hora']}</td>
                  <td>{r['bairro_id']}</td>
                  <td><span class="tag tag-{ 'ok' if r['status_bloco']=='ACESSO_VALIDADO_REGIONALMENTE' else ('alerta' if r['status_bloco']=='ACESSO_NAO_CONFIRMADO' else 'normal') }">{r['status_bloco']}</span></td>
                  <td class="mono">{fmt_acessos(r['acessos_validados'] + r['acessos_nao_confirmados'])}</td>
                  <td class="mono">{r['eventos_energia']}</td>
                </tr>"""
            for r in reversed(self.historico)
        ) or "<tr><td colspan='5'>Nenhum relatório recebido ainda.</td></tr>"

        linhas_barras = "\n".join(
            f"""<div class="linha-barra">
                  <span class="rotulo-barra mono">{status}</span>
                  <div class="trilha-barra"><div class="barra" style="width:{(qtd/max_contagem)*100:.0f}%"></div></div>
                  <span class="valor-barra mono">{qtd}</span>
                </div>"""
            for status, qtd in contagem_status.most_common()
        )

        if self.ajustes_modelo:
            linhas_ajustes = "\n".join(
                f"<li><span class='mono'>{a['hora']}</span> — modelo de visão atualizado para a "
                f"versão <strong>{a['versao']:.1f}</strong> · {a['amostras_usadas']} placas usadas "
                f"no re-treino · aplicado a {a['gateways_atualizados']} gateway(s) Edge e "
                f"{a['nos_fog_atualizados']} nó(s) de Fog</li>"
                for a in self.ajustes_modelo
            )
            bloco_ml = f"<ul class='lista-ajustes'>{linhas_ajustes}</ul>"
        else:
            bloco_ml = "<p class='texto-vazio'>Nenhum re-treino do modelo de visão ocorreu ainda nesta simulação.</p>"

        html = f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="UTF-8">
<title>SafeLiving — Gestão de Propriedades e Segurança Global</title>
<style>
  :root {{
    --bg: #0D1B2A;
    --bg-panel: #14283D;
    --linha: #24405C;
    --texto: #DCE6F0;
    --texto-suave: #8FA6BE;
    --ambar: #E3A008;
    --ciano: #22B8CF;
    --vermelho: #E0563F;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    background: var(--bg);
    color: var(--texto);
    font-family: 'Segoe UI', Roboto, -apple-system, sans-serif;
    margin: 0;
    padding: 40px 24px 80px;
  }}
  .mono {{ font-family: 'Consolas', 'SF Mono', monospace; }}
  .container {{ max-width: 940px; margin: 0 auto; }}
  header h1 {{ font-size: 26px; font-weight: 600; margin: 0 0 4px; letter-spacing: -0.01em; }}
  header p {{ color: var(--texto-suave); margin: 0 0 32px; font-size: 15px; }}
  .kpis {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1px;
    background: var(--linha);
    border: 1px solid var(--linha);
    margin-bottom: 40px;
  }}
  .kpi {{ background: var(--bg-panel); padding: 20px; }}
  .kpi .numero {{ font-size: 30px; font-weight: 600; font-family: 'Consolas', 'SF Mono', monospace; }}
  .kpi .rotulo {{ color: var(--texto-suave); font-size: 13px; margin-top: 4px; }}
  section {{ margin-bottom: 40px; }}
  section h2 {{
    font-size: 15px; font-weight: 600; color: var(--texto-suave);
    border-bottom: 1px solid var(--linha); padding-bottom: 10px; margin-bottom: 16px;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th {{ text-align: left; color: var(--texto-suave); font-weight: 500; padding: 8px 10px; border-bottom: 1px solid var(--linha); font-size: 12px; }}
  td {{ padding: 9px 10px; border-bottom: 1px solid var(--linha); }}
  .tag {{ padding: 3px 8px; font-size: 12px; border: 1px solid; }}
  .tag-ok {{ color: var(--ciano); border-color: var(--ciano); }}
  .tag-alerta {{ color: var(--vermelho); border-color: var(--vermelho); }}
  .tag-normal {{ color: var(--texto-suave); border-color: var(--linha); }}
  .linha-barra {{ display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }}
  .rotulo-barra {{ width: 260px; font-size: 12px; color: var(--texto-suave); flex-shrink: 0; }}
  .trilha-barra {{ flex: 1; background: var(--bg-panel); height: 10px; }}
  .barra {{ background: var(--ambar); height: 100%; }}
  .valor-barra {{ width: 24px; text-align: right; font-size: 13px; }}
  .lista-ajustes {{ list-style: none; padding: 0; margin: 0; font-size: 14px; line-height: 2; }}
  .lista-ajustes li {{ border-bottom: 1px solid var(--linha); padding: 10px 0; }}
  .texto-vazio {{ color: var(--texto-suave); font-size: 14px; }}
  footer {{ color: var(--texto-suave); font-size: 12px; margin-top: 40px; }}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>SafeLiving — Gestão de Propriedades e Segurança Global</h1>
    <p>Gerado automaticamente pela Cloud ao final da simulação · {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
  </header>

  <div class="kpis">
    <div class="kpi"><div class="numero">{len(self.historico)}</div><div class="rotulo">relatórios recebidos</div></div>
    <div class="kpi"><div class="numero">{len(bairros_monitorados)}</div><div class="rotulo">bairros monitorados</div></div>
    <div class="kpi"><div class="numero">{total_validados}</div><div class="rotulo">acessos validados</div></div>
    <div class="kpi"><div class="numero">{total_nao_confirmados}</div><div class="rotulo">acessos não confirmados</div></div>
  </div>

  <section>
    <h2>Linha do tempo de eventos (mais recente primeiro)</h2>
    <table>
      <thead><tr><th>Hora</th><th>Bairro</th><th>Status</th><th>Casa (placa)</th><th>Eventos de energia</th></tr></thead>
      <tbody>{linhas_timeline}</tbody>
    </table>
  </section>

  <section>
    <h2>Distribuição de status recebidos</h2>
    {linhas_barras}
  </section>

  <section>
    <h2>Modelo de visão computacional — o que a Cloud aprendeu</h2>
    {bloco_ml}
  </section>

  <footer>Dados fictícios gerados para fins didáticos — simulador SafeLiving Edge/Fog/Cloud.</footer>
</div>
</body>
</html>"""

        caminho = os.path.join(os.getcwd(), NOME_ARQUIVO_DASHBOARD)
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"\n[CLOUD] Dashboard aberto em uma página separada: {caminho}")
        try:
            webbrowser.open(f"file://{caminho}")
        except Exception:
            pass
        return caminho

    # ======================================================================
    # MENU DE EXPLORAÇÃO DOS DADOS COLETADOS (no terminal)
    # ======================================================================
    def menu_consultas(self):
        if not self.historico:
            return

        opcoes = """
  Explorar os dados coletados pela Cloud:
  1 - Ver a linha do tempo completa de eventos
  2 - Filtrar eventos por bairro
  3 - Filtrar eventos por casa
  4 - Ver as atualizações do modelo de visão computacional
  0 - Sair
  Escolha: """

        while True:
            try:
                escolha = input(opcoes).strip()
            except EOFError:
                break

            if escolha == "1":
                for r in self.historico:
                    acessos = r["acessos_validados"] + r["acessos_nao_confirmados"]
                    print(f"   {r['hora']} | {r['bairro_id']} | {r['status_bloco']} | "
                          f"acessos={[(a['casa_numero'], a['placa']) for a in acessos]}")

            elif escolha == "2":
                alvo = input("  Digite o id do bairro (ex.: bloco-A): ").strip()
                encontrados = [r for r in self.historico if r["bairro_id"] == alvo]
                if not encontrados:
                    print(f"  Nenhum evento encontrado para '{alvo}'.")
                for r in encontrados:
                    print(f"   {r['hora']} | {r['status_bloco']} | "
                          f"validados={r['acessos_validados']}")

            elif escolha == "3":
                alvo = input("  Digite o número da casa (ex.: 104): ").strip()
                encontrados = []
                for r in self.historico:
                    for a in r["acessos_validados"] + r["acessos_nao_confirmados"]:
                        if str(a["casa_numero"]) == alvo:
                            encontrados.append((r["hora"], r["status_bloco"], a["placa"]))
                if not encontrados:
                    print(f"  Nenhum acesso encontrado para a casa #{alvo}.")
                for hora, status, placa in encontrados:
                    print(f"   {hora} | placa {placa} | status do bairro: {status}")

            elif escolha == "4":
                if not self.ajustes_modelo:
                    print("  O modelo de visão ainda não foi re-treinado nesta simulação.")
                for a in self.ajustes_modelo:
                    print(f"   {a['hora']} | versão {a['versao']:.1f} | "
                          f"{a['amostras_usadas']} amostras | "
                          f"{a['gateways_atualizados']} gateways + {a['nos_fog_atualizados']} nós de Fog atualizados")

            elif escolha == "0":
                print("  Encerrando.")
                break

            else:
                print("  Opção inválida, tente novamente.")


# ======================================================================
# MONTAGEM DA SIMULAÇÃO
# ======================================================================
def montar_ambiente():
    tipos = ["camera_garagem", "sensor_presenca_garagem", "medidor_energia"]

    def criar_casa(numero, bairro_id):
        dispositivos = [DispositivoResidencial(f"D{numero}{t}", t, casa_numero=numero) for t in tipos]
        return CasaInteligente(casa_numero=numero, bairro_id=bairro_id, dispositivos=dispositivos)

    bloco_a = EstacaoORAN("bloco-A", casas_atendidas=[88, 104, 207])
    bloco_b = EstacaoORAN("bloco-B", casas_atendidas=[150, 151])

    casas_bloco_a = [criar_casa(n, "bloco-A") for n in bloco_a.casas_atendidas]
    casas_bloco_b = [criar_casa(n, "bloco-B") for n in bloco_b.casas_atendidas]

    fog_nodes = [bloco_a, bloco_b]
    todas_casas = casas_bloco_a + casas_bloco_b
    nucleo = NucleoCentral(fog_nodes=fog_nodes, casas=todas_casas)

    return {"bloco-A": (bloco_a, casas_bloco_a), "bloco-B": (bloco_b, casas_bloco_b)}, nucleo


def rodar_simulacao():
    bairros, nucleo = montar_ambiente()

    for ciclo in range(1, CICLOS_DE_SIMULACAO + 1):
        print(f"\n===== CICLO {ciclo} =====")

        for bairro_id, (fog, casas) in bairros.items():
            print(f"  -- {bairro_id} --")
            for casa in casas:
                eventos = casa.ciclo(ciclo)
                fog.receber(eventos)

            relatorio = fog.ciclo(ciclo)
            if relatorio:
                nucleo.registrar(relatorio)

    nucleo.gerar_dashboard_html()
    nucleo.menu_consultas()


if __name__ == "__main__":
    rodar_simulacao()


# ======================================================================
# SUGESTÕES DE MUDANÇAS PARA OS ALUNOS FAZEREM EM SALA
# ======================================================================
# 1. Adicionar uma nova chegada em SCRIPT_CHEGADAS para uma casa nova e
#    ver o dashboard e o menu refletirem esse novo acesso.
#
# 2. Mudar LIMIAR_RETREINO_VISAO para 2 e observar o modelo de visão
#    sendo atualizado mais cedo (com menos placas capturadas).
#
# 3. Fazer a Casa #88 (a que não é confirmada pelo trânsito público)
#    também virar CONFIRMADA, adicionando sua entrada em
#    PLACAS_CONFIRMADAS_TRANSITO_PUBLICO, e comparar o status_bloco
#    resultante.
#
# 4. Abrir o dashboard_safeliving.html e adicionar um novo KPI (ex.:
#    quantos acessos aconteceram por bairro).
#
# 5. Criar uma nova opção no menu_consultas() que mostre só os acessos
#    NÃO confirmados regionalmente, para discutir o que a SafeLiving
#    poderia fazer nesse caso (ex.: notificação de segurança extra).
# ======================================================================
