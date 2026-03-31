import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go
import re
import gspread
from google.oauth2.service_account import Credentials
import json

st.set_page_config(page_title="WhatsApp de Obra", page_icon="💬", layout="wide")

hoy = datetime.date.today()
hoy_str = hoy.strftime("%Y-%m-%d")
COLORES = ["#00a884","#128c7e","#25d366","#34b7f1","#075e54","#dcf8c6","#ff6b35","#f7c59f"]

# ═══════════════════════════════════════════════════════════
# CONEXIÓN + CRUD
# ═══════════════════════════════════════════════════════════
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

@st.cache_resource(ttl=300)
def conectar_gsheets():
    #creds = Credentials.from_service_account_file("credenciales.json", scopes=SCOPES)
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds).open("ObraApp")

def _leer(libro, hoja):
    try: ws = libro.worksheet(hoja)
    except gspread.exceptions.WorksheetNotFound: return pd.DataFrame()
    try:
        r = ws.get_all_records(default_blank="")
        if r: return pd.DataFrame(r)
    except Exception: pass
    t = ws.get_all_values()
    if len(t) <= 1: return pd.DataFrame()
    enc = t[0]
    while enc and not enc[-1].strip(): enc.pop()
    n = len(enc)
    if n == 0: return pd.DataFrame()
    datos = [f[:n] for f in t[1:] if any(c.strip() for c in f[:n])]
    return pd.DataFrame(datos, columns=enc) if datos else pd.DataFrame()

def _escribir_hoja(libro, hoja):
    try: return libro.worksheet(hoja)
    except gspread.exceptions.WorksheetNotFound: return libro.add_worksheet(title=hoja, rows=1000, cols=20)

def agregar_fila(libro, hoja, fila):
    _escribir_hoja(libro, hoja).append_row(fila, value_input_option="USER_ENTERED")

def reemplazar_filas_dia(libro, hoja, proyecto, fecha_str, nuevas):
    ws = _escribir_hoja(libro, hoja)
    t = ws.get_all_values()
    if len(t) <= 1:
        ws.append_rows(nuevas, value_input_option="USER_ENTERED"); return
    mantener = [t[0]] + [f for f in t[1:] if not (f[0]==proyecto and f[1]==fecha_str)]
    ws.clear()
    ws.update(range_name="A1", values=mantener, value_input_option="USER_ENTERED")
    ws.append_rows(nuevas, value_input_option="USER_ENTERED")

# ═══════════════════════════════════════════════════════════
# FORMATOS COLOMBIANOS
# ═══════════════════════════════════════════════════════════
def parsear_moneda(v):
    if v is None: return 0.0
    s = str(v).strip().replace("$","").replace(" ","")
    if not s: return 0.0
    if "." in s and "," not in s:
        p = s.split(".")
        if len(p)>2 or (len(p)==2 and len(p[-1])==3): s = s.replace(".","")
    elif "," in s and "." not in s:
        p = s.split(",")
        if len(p)>2 or (len(p)==2 and len(p[-1])==3): s = s.replace(",","")
        else: s = s.replace(",",".")
    elif "." in s and "," in s:
        if s.rfind(",") > s.rfind("."): s = s.replace(".","").replace(",",".")
        else: s = s.replace(",","")
    try: return float(s)
    except ValueError: return 0.0

def fmt_cop(v):
    try: n = float(v)
    except: return "$0"
    return "$" + f"{n:,.0f}".replace(",",".")

def fmt_dec(v, d=2):
    try: n = float(v)
    except: return "0"
    t = f"{n:,.{d}f}"
    return t.replace(",","TEMP").replace(".",",").replace("TEMP",".")

def _norm(df):
    df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]
    return df

# ═══════════════════════════════════════════════════════════
# SESSION STATE + CACHE CENTRALIZADO
# ═══════════════════════════════════════════════════════════
_defaults = {
    "chat_historial": {}, "nombre_proyecto": None, "acceso_rapido_modo": None,
    "usuario_actual": None, "rol_actual": None,
    "datos_proyecto": None, "resumen_fin": {},
    "c_actividades": None, "c_trabajadores": None, "c_avances": None,
    "c_materiales": None, "c_asistencia": None, "c_nomina": None, "c_tareas": None,
    "ultima_carga": None,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

def es_director(): return st.session_state.rol_actual == "Director"
def es_residente(): return st.session_state.rol_actual == "Residente"
def logueado(): return st.session_state.usuario_actual is not None
def usr_nombre(): return st.session_state.get("nombre_visible", st.session_state.usuario_actual or "Sistema")

# ── Cargar TODOS los datos en un solo paso ──
def cargar_todo(libro, forzar=False):
    """Solo carga si no hay datos o si se fuerza. NO recarga automaticamente."""
    if not forzar and st.session_state.datos_proyecto:
        return  # ya hay datos, no recargar

    # 1) Config del proyecto
    try:
        ws = libro.worksheet("config")
        rows = ws.get_all_values()
        dp = {}
        for r in rows:
            if len(r) >= 2:
                k = str(r[0]).strip().lower().replace(" ","_")
                val = str(r[1]).strip()
                if k and val: dp[k] = val
    except gspread.exceptions.WorksheetNotFound:
        dp = {}
    st.session_state.datos_proyecto = dp
    st.session_state.nombre_proyecto = dp.get("proyecto", "Sin nombre")

    rf = {}
    for ki, ks in [("costo_total_del_proyecto","costo_total_proyecto"),("total_costo_directo","total_costo_directo"),("total_costo_suministros","total_costo_suministros"),("costo_total_de_la_obra","costo_total_obra")]:
        rf[ki] = parsear_moneda(dp.get(ks, 0))
    st.session_state.resumen_fin = rf

    # 2) Actividades
    df = _leer(libro, "actividades")
    if not df.empty:
        df = _norm(df)
        for c in ["valor_unitario","cantidad_total","valor_total"]:
            if c in df.columns: df[c] = df[c].apply(parsear_moneda)
    st.session_state.c_actividades = df

    # 3) Trabajadores
    dft = _leer(libro, "trabajadores")
    if not dft.empty:
        dft = _norm(dft)
        col_activo = "activo" if "activo" in dft.columns else ("estado" if "estado" in dft.columns else None)
        if col_activo:
            dft = dft[dft[col_activo].astype(str).str.upper() == "SI"]
    st.session_state.c_trabajadores = dft.to_dict("records") if not dft.empty else []

    # 4) Avances
    dfa = _leer(libro, "avances")
    if not dfa.empty:
        dfa = _norm(dfa)
        if "cantidad" in dfa.columns: dfa["cantidad"] = dfa["cantidad"].apply(parsear_moneda)
    st.session_state.c_avances = dfa

    # 5) Materiales
    dfm = _leer(libro, "materiales")
    if not dfm.empty: dfm = _norm(dfm)
    st.session_state.c_materiales = dfm

    # 6) Asistencia
    dfas = _leer(libro, "asistencia")
    if not dfas.empty: dfas = _norm(dfas)
    st.session_state.c_asistencia = dfas

    # 7) Nomina
    dfn = _leer(libro, "nomina")
    if not dfn.empty:
        dfn = _norm(dfn)
        if "valor" in dfn.columns: dfn["valor"] = dfn["valor"].apply(parsear_moneda)
    st.session_state.c_nomina = dfn

    # 8) Tareas
    dfta = _leer(libro, "tareas")
    if not dfta.empty: dfta = _norm(dfta)
    st.session_state.c_tareas = dfta

    st.session_state.ultima_carga = datetime.datetime.now()


# ── Helpers para actualizar cache local sin releer Sheets ──
def _cache_append(cache_key, new_row_dict):
    """Agrega una fila al DataFrame cacheado sin llamar a la API."""
    df = st.session_state.get(cache_key)
    if df is None or df.empty:
        st.session_state[cache_key] = pd.DataFrame([new_row_dict])
    else:
        st.session_state[cache_key] = pd.concat([df, pd.DataFrame([new_row_dict])], ignore_index=True)

def _cache_replace_dia(cache_key, proyecto, fecha, nuevas_filas_dict):
    """Reemplaza filas del dia en el cache local."""
    df = st.session_state.get(cache_key)
    if df is None or df.empty:
        st.session_state[cache_key] = pd.DataFrame(nuevas_filas_dict)
    else:
        mask = ~((df["proyecto"]==proyecto) & (df["fecha"]==fecha))
        st.session_state[cache_key] = pd.concat([df[mask], pd.DataFrame(nuevas_filas_dict)], ignore_index=True)

# ── Getters del cache ──
def get_actividades(): return st.session_state.c_actividades if st.session_state.c_actividades is not None else pd.DataFrame()
def get_trabajadores(): return st.session_state.c_trabajadores or []
def get_avances(proy=None):
    df = st.session_state.c_avances if st.session_state.c_avances is not None else pd.DataFrame()
    if proy and not df.empty and "proyecto" in df.columns: df = df[df["proyecto"]==proy]
    return df
def get_materiales(proy=None):
    df = st.session_state.c_materiales if st.session_state.c_materiales is not None else pd.DataFrame()
    if proy and not df.empty and "proyecto" in df.columns: df = df[df["proyecto"]==proy]
    return df
def get_asistencia(proy=None):
    df = st.session_state.c_asistencia if st.session_state.c_asistencia is not None else pd.DataFrame()
    if proy and not df.empty and "proyecto" in df.columns: df = df[df["proyecto"]==proy]
    return df
def get_nomina(proy=None):
    df = st.session_state.c_nomina if st.session_state.c_nomina is not None else pd.DataFrame()
    if proy and not df.empty and "proyecto" in df.columns: df = df[df["proyecto"]==proy]
    return df
def get_tareas(proy=None):
    df = st.session_state.c_tareas if st.session_state.c_tareas is not None else pd.DataFrame()
    if proy and not df.empty and "proyecto" in df.columns: df = df[df["proyecto"]==proy]
    return df

def calcular_avances(df_act, df_av):
    dr = df_act.copy()
    for c in ["valor_unitario","cantidad_total","valor_total"]:
        if c in dr.columns: dr[c] = pd.to_numeric(dr[c], errors="coerce").fillna(0)
    dr["cantidad_ejecutada"] = 0.0
    if not df_av.empty and "id_item" in df_av.columns:
        ag = df_av.groupby("id_item")["cantidad"].sum().reset_index()
        ag.columns = ["id","cantidad_ejecutada"]
        dr = dr.merge(ag, on="id", how="left", suffixes=("","_n"))
        if "cantidad_ejecutada_n" in dr.columns:
            dr["cantidad_ejecutada"] = dr["cantidad_ejecutada_n"].fillna(0)
            dr.drop(columns=["cantidad_ejecutada_n"], inplace=True)
    dr["cantidad_ejecutada"] = dr["cantidad_ejecutada"].fillna(0)
    dr["pct_avance"] = dr.apply(lambda r: min(round(r["cantidad_ejecutada"]/r["cantidad_total"]*100,1) if r["cantidad_total"]>0 else 0, 100), axis=1)
    dr["valor_ejecutado"] = dr["cantidad_ejecutada"] * dr["valor_unitario"]
    return dr

def generar_id_tarea(libro, proy):
    df = get_tareas(proy)
    if df.empty: return "T-001"
    nums = [int(m.group(1)) for t in df["id_tarea"].astype(str) if (m := re.search(r'(\d+)', t))]
    return f"T-{(max(nums)+1 if nums else 1):03d}"

# ═══════════════════════════════════════════════════════════
# CONEXIÓN
# ═══════════════════════════════════════════════════════════
try:
    libro = conectar_gsheets(); conexion_ok = True
except Exception as e:
    conexion_ok = False; error_conexion = str(e)

# ═══════════════════════════════════════════════════════════
# MOTOR DE CHAT
# ═══════════════════════════════════════════════════════════
def procesar_mensaje(texto, proy, libro):
    tl = texto.lower(); ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M"); u = usr_nombre()
    if "avance" in tl:
        m = re.search(r'(NP\d+|\d+\.\d+\.\d+|[a-zA-Z]{2,5}-\d{2,3})', texto)
        if m:
            iid = m.group(1); ts = texto.replace(iid,""); cm = re.search(r'(\d+(\.\d+)?)', ts)
            if cm:
                cant = float(cm.group(1)); ids = list(get_actividades()["id"].values) if not get_actividades().empty and "id" in get_actividades().columns else []
                if iid in ids:
                    agregar_fila(libro,"avances",[proy,hoy_str,iid,cant,u,ahora])
                    _cache_append("c_avances",{"proyecto":proy,"fecha":hoy_str,"id_item":iid,"cantidad":cant,"usuario":u,"timestamp":ahora})
                    return f"<b>Avance registrado:</b> {cant} al item <b>{iid}</b>."
                return f"Item <b>{iid}</b> no existe."
            return "Falta la <b>cantidad</b>. Ej: 'Avance 1.1.01 15'."
        return "Falta el <b>codigo</b>. Ej: 'Avance 1.1.01 15'."
    elif "material" in tl or "necesito" in tl:
        req = re.sub(r'(?i)(material|necesito)','',texto).strip()
        agregar_fila(libro,"materiales",[proy,hoy_str,req,"Solicitado",u,ahora])
        _cache_append("c_materiales",{"proyecto":proy,"fecha":hoy_str,"requerimiento":req,"estado":"Solicitado","usuario":u,"timestamp":ahora})
        return f"<b>Material solicitado:</b> '{req}'."
    elif "asistencia" in tl:
        c = re.sub(r'(?i)asistencia[:\s]*','',texto).strip(); p = [x.strip() for x in c.split('-')]
        if len(p)>=2:
            estado = p[2] if len(p)>2 else "Presente"
            agregar_fila(libro,"asistencia",[proy,hoy_str,p[0],p[1],estado,u,ahora])
            _cache_append("c_asistencia",{"proyecto":proy,"fecha":hoy_str,"trabajador":p[0],"cargo":p[1],"estado":estado,"usuario":u,"timestamp":ahora})
            return f"<b>Asistencia:</b> {p[0]} — <b>{estado}</b>."
        return "Formato: <b>Asistencia: Nombre - Cargo - Estado</b>."
    return "Usa los accesos rapidos o palabras clave: <b>Avance</b>, <b>Material</b>, <b>Asistencia</b>."

# ═══════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════
st.markdown("""<style>
.stApp{background-color:#efeae2!important;background-image:url("https://user-images.githubusercontent.com/15075759/28719144-86dc0f70-73b1-11e7-911d-60d70fcded21.png");background-repeat:repeat;background-attachment:fixed}
header[data-testid="stHeader"]{background-color:transparent!important}
[data-testid="stChatInput"]{background-color:transparent!important;padding-bottom:20px!important}
[data-testid="stChatInput"]>div{background-color:#fff!important;border-radius:20px!important;border:none!important;box-shadow:0 1px 3px rgba(0,0,0,.15)!important}
.kpi-card{background:#fff;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-left:5px solid #00a884;margin-bottom:10px}
.kpi-card.naranja{border-left-color:#ff6b35}.kpi-card.azul{border-left-color:#34b7f1}.kpi-card.rojo{border-left-color:#e53935}
.kpi-valor{font-size:28px;font-weight:700;color:#111b21;font-family:'Segoe UI',sans-serif}
.kpi-label{font-size:13px;color:#667781;font-family:'Segoe UI',sans-serif;margin-top:2px}
.section-header{font-size:17px;font-weight:600;color:#075e54;font-family:'Segoe UI',sans-serif;padding:8px 0 4px;border-bottom:2px solid #00a884;margin-bottom:14px}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════
# LOGIN
# ═══════════════════════════════════════════════════════════
if not logueado():
    st.sidebar.markdown("""<div style="background:#075e54;padding:20px;border-radius:8px;margin-bottom:20px;text-align:center">
        <div style="font-size:28px;margin-bottom:8px">🏗️</div>
        <div style="color:#fff;font-size:18px;font-weight:700">ObraApp</div>
        <div style="color:#b2dfdb;font-size:12px">Inicia sesion</div></div>""", unsafe_allow_html=True)
    if conexion_ok:
        dfu = _leer(libro,"usuarios")
        if dfu.empty: st.sidebar.warning("Crea la hoja 'usuarios'"); st.stop()
        dfu = _norm(dfu)
        lu = st.sidebar.text_input("Usuario:",key="lu"); lp = st.sidebar.text_input("Clave:",type="password",key="lp")
        if st.sidebar.button("Ingresar",type="primary",use_container_width=True,key="btn_login"):
            m = dfu[(dfu["usuario"].astype(str).str.strip()==lu.strip())&(dfu["clave"].astype(str).str.strip()==lp.strip())]
            if not m.empty:
                r = m.iloc[0]; st.session_state.usuario_actual = str(r["usuario"]).strip()
                st.session_state.rol_actual = str(r["rol"]).strip()
                st.session_state["nombre_visible"] = str(r.get("nombre_visible",r["usuario"])).strip()
                st.rerun()
            else: st.sidebar.error("Credenciales incorrectas.")
    else: st.sidebar.error(f"Sin conexion: {error_conexion}")
    st.stop()

# ═══════════════════════════════════════════════════════════
# SIDEBAR (logueado)
# ═══════════════════════════════════════════════════════════
color_r = "#075e54" if es_director() else "#00a884"
icon_r = "👔" if es_director() else "👷‍♂️"
st.sidebar.markdown(f"""<div style="background:#f0f2f5;padding:15px;border-radius:5px;margin-bottom:20px;display:flex;align-items:center">
    <div style="background:{color_r};color:#fff;border-radius:50%;width:40px;height:40px;display:flex;align-items:center;justify-content:center;font-size:20px;margin-right:15px">{icon_r}</div>
    <div><h3 style="margin:0;font-size:15px;color:#111b21">{usr_nombre()}</h3><p style="margin:0;font-size:11px;color:#667781">{st.session_state.rol_actual}</p></div></div>""", unsafe_allow_html=True)

menu_ops = ["💬 Chat de Obra","📊 Panel del Director","🏢 Proveedores"] if es_director() else ["💬 Chat de Obra","📋 Mis Tareas"]
menu = st.sidebar.selectbox("Navegacion", menu_ops)

sc1,sc2 = st.sidebar.columns(2)
with sc1:
    if st.button("Cerrar sesion",use_container_width=True,key="btn_logout"):
        for k in list(_defaults.keys())+["nombre_visible"]: st.session_state.pop(k, None)
        for k,v in _defaults.items(): st.session_state[k] = v
        st.rerun()
with sc2:
    if st.button("Recargar datos",use_container_width=True,key="btn_reload"):
        st.session_state.ultima_carga = None; cargar_todo(libro,forzar=True); st.rerun()

if conexion_ok:
    cargar_todo(libro)
    proy = st.session_state.nombre_proyecto
    rf = st.session_state.resumen_fin
    if proy=="Sin nombre": st.sidebar.warning("Verifica hoja 'config'.")
    st.sidebar.markdown(f"""<div style="background:#e8f5e9;border-radius:8px;padding:10px 12px;margin-top:8px;font-family:'Segoe UI',sans-serif">
        <div style="font-size:12px;color:#075e54;font-weight:600">📋 {proy[:35]}</div>
        <div style="font-size:11px;color:#667781;margin-top:4px">Proyecto: <b>{fmt_cop(rf.get('costo_total_del_proyecto',0))}</b></div>
        <div style="font-size:11px;color:#667781">Directo: <b>{fmt_cop(rf.get('total_costo_directo',0))}</b></div></div>""", unsafe_allow_html=True)
else: st.sidebar.error(f"Sin conexion: {error_conexion}"); st.stop()


# ═══════════════════════════════════════════════════════════
# CHAT DE OBRA
# ═══════════════════════════════════════════════════════════
if menu == "💬 Chat de Obra":
    PA = st.session_state.nombre_proyecto
    if PA not in st.session_state.chat_historial:
        st.session_state.chat_historial[PA] = [{"role":"assistant","content":f"Proyecto <b>{PA}</b> conectado. Usa los accesos rapidos."}]

    st.markdown(f"""<div style="background:#f0f2f5;padding:10px 16px;display:flex;align-items:center;border-bottom:1px solid #d1d7db;margin-top:-60px;margin-bottom:20px;position:sticky;top:0;z-index:999">
        <div style="background:#dfe5e7;border-radius:50%;width:45px;height:45px;display:flex;align-items:center;justify-content:center;font-size:22px;margin-right:15px">🏗️</div>
        <div><h4 style="margin:0;font-size:16px;color:#111b21">{PA}</h4><p style="margin:0;font-size:13px;color:#667781">Bot Asistente</p></div></div>""", unsafe_allow_html=True)

    for msg in st.session_state.chat_historial[PA]:
        bg="#d9fdd3" if msg["role"]=="user" else "#fff"
        al="flex-end" if msg["role"]=="user" else "flex-start"
        rd="12px 0 12px 12px" if msg["role"]=="user" else "0 12px 12px 12px"
        st.markdown(f'<div style="display:flex;justify-content:{al};margin-bottom:12px"><div style="background:{bg};padding:10px 14px;border-radius:{rd};box-shadow:0 1px 1px rgba(0,0,0,.15);max-width:75%;font-size:15px;color:#111b21;line-height:1.4">{msg["content"]}</div></div>', unsafe_allow_html=True)

    modo = st.session_state.acceso_rapido_modo
    st.markdown('<div style="background:#f0f2f5;border-radius:12px;padding:10px 14px;margin:8px 0 4px;border:1px solid #d1d7db"><div style="font-size:12px;color:#667781;margin-bottom:8px"><b>Accesos rapidos</b></div>', unsafe_allow_html=True)

    def _tb(col,on,off,val,key):
        with col:
            if st.button(on if modo==val else off,use_container_width=True,key=key,type="primary" if modo==val else "secondary"):
                st.session_state.acceso_rapido_modo = None if modo==val else val; st.rerun()

    if es_director():
        r1,r2,r3 = st.columns(3); r4,r5,r6 = st.columns(3)
        _tb(r1,">> Avance","Reportar Avance","avance","ba"); _tb(r2,">> Material","Solicitar Material","material","bm"); _tb(r3,">> Asistencia","Control Asistencia","asistencia","bas")
        _tb(r4,">> Pago","Nomina / Pagos","nomina","bn"); _tb(r5,">> Tarea","Asignar Tarea","tarea","bt")
        with r6:
            if st.button("Novedad",use_container_width=True,key="bman",type="primary" if modo is None else "secondary"): st.session_state.acceso_rapido_modo=None; st.rerun()
    else:
        r1,r2,r3 = st.columns(3); r4,r5,_ = st.columns(3)
        _tb(r1,">> Avance","Reportar Avance","avance","ba"); _tb(r2,">> Material","Solicitar Material","material","bm"); _tb(r3,">> Asistencia","Control Asistencia","asistencia","bas")
        _tb(r4,">> Mis Tareas","Mis Tareas","mis_tareas","bmt")
        with r5:
            if st.button("Novedad",use_container_width=True,key="bman",type="primary" if modo is None else "secondary"): st.session_state.acceso_rapido_modo=None; st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    # ── AVANCE (multiple items) ──
    if modo=="avance":
        st.markdown('<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#075e54;border-bottom:2px solid #00a884;padding-bottom:8px;margin-bottom:16px">Reportar Avance</div>', unsafe_allow_html=True)
        df_acts = get_actividades()
        if not df_acts.empty and "capitulo" in df_acts.columns:
            # Filtros
            f1,f2 = st.columns(2)
            with f1: cap = st.selectbox("Capitulo:", sorted(df_acts["capitulo"].unique().tolist()), key="ac")
            with f2: bus = st.text_input("Buscar:", key="ab")
            dff = df_acts[df_acts["capitulo"]==cap].copy()
            if bus: dff = dff[dff["descripcion"].str.contains(bus,case=False,na=False)]

            if dff.empty:
                st.info("Sin resultados.")
            else:
                # Calcular ejecutado previo para cada actividad
                avp = get_avances(PA)
                ejecutados = {}
                if not avp.empty and "id_item" in avp.columns:
                    ejecutados = avp.groupby("id_item")["cantidad"].sum().to_dict()

                # Construir tabla editable
                filas_tabla = []
                for _, r in dff.iterrows():
                    ej = ejecutados.get(r["id"], 0.0)
                    ct = float(r["cantidad_total"]) if r["cantidad_total"] > 0 else 0
                    pe = max(ct - ej, 0)
                    pc = min(round(ej/ct*100, 1), 100) if ct > 0 else 0
                    filas_tabla.append({
                        "ID": r["id"],
                        "Descripcion": str(r["descripcion"])[:50],
                        "Unidad": r["unidad"],
                        "Cant. Total": ct,
                        "Ejecutado": ej,
                        "Pendiente": pe,
                        "% Avance": pc,
                        "Reportar hoy": 0.0,
                    })

                df_reporte = pd.DataFrame(filas_tabla)

                # KPIs del capítulo
                tot_cap = len(df_reporte)
                con_av = int((df_reporte["Ejecutado"] > 0).sum())
                # Avance ponderado del capitulo
                vt_cap = df_reporte["Cant. Total"].sum()
                pct_cap = round((df_reporte["% Avance"] * df_reporte["Cant. Total"]).sum() / vt_cap, 1) if vt_cap > 0 else 0
                k1,k2,k3 = st.columns(3)
                for c,v,l,cl in [(k1,tot_cap,"Actividades","#34b7f1"),(k2,con_av,"Con avance","#00a884"),(k3,f"{fmt_dec(pct_cap,1)}%","Avance capitulo","#ff6b35")]:
                    c.markdown(f'<div style="background:#f8f9fa;border-radius:8px;padding:8px 10px;text-align:center;border-top:3px solid {cl};margin-bottom:10px"><div style="font-size:16px;font-weight:700">{v}</div><div style="font-size:10px;color:#667781">{l}</div></div>', unsafe_allow_html=True)

                st.markdown("<div style='font-size:13px;color:#667781;margin-bottom:8px'>Ingresa las cantidades en la columna <b>Reportar hoy</b> y presiona <b>Guardar avances</b>:</div>", unsafe_allow_html=True)

                df_editado = st.data_editor(
                    df_reporte,
                    column_config={
                        "ID": st.column_config.TextColumn("ID", disabled=True, width="small"),
                        "Descripcion": st.column_config.TextColumn("Actividad", disabled=True, width="large"),
                        "Unidad": st.column_config.TextColumn("Und", disabled=True, width="small"),
                        "Cant. Total": st.column_config.NumberColumn("Total", disabled=True, format="%.2f"),
                        "Ejecutado": st.column_config.NumberColumn("Ejecutado", disabled=True, format="%.2f"),
                        "Pendiente": st.column_config.NumberColumn("Pendiente", disabled=True, format="%.2f"),
                        "% Avance": st.column_config.ProgressColumn("% Avance", min_value=0, max_value=100, format="%.1f%%"),
                        "Reportar hoy": st.column_config.NumberColumn("Reportar hoy", min_value=0.0, format="%.2f", width="medium"),
                    },
                    hide_index=True, use_container_width=True, key="df_avance_multi"
                )

                # Filtrar solo filas con reporte > 0
                df_con_reporte = df_editado[df_editado["Reportar hoy"] > 0]
                n_reportes = len(df_con_reporte)

                # Resumen de lo que se va a enviar
                if n_reportes > 0:
                    st.markdown(f"<div style='background:#e8f5e9;border-radius:8px;padding:10px 12px;margin:8px 0;font-size:13px;color:#075e54'><b>{n_reportes} actividad(es)</b> con avance para registrar</div>", unsafe_allow_html=True)

                _,bc = st.columns([3,1])
                with bc:
                    if st.button("Guardar avances", use_container_width=True, key="aen_multi", type="primary", disabled=(n_reportes == 0)):
                        ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                        u = usr_nombre()
                        filas_sheets = []
                        resumen_items = []
                        for _, row in df_con_reporte.iterrows():
                            filas_sheets.append([PA, hoy_str, row["ID"], row["Reportar hoy"], u, ahora])
                            resumen_items.append(f"{row['ID']}: {fmt_dec(row['Reportar hoy'])} {row['Unidad']}")

                        # Escribir todas las filas de una vez (1 sola llamada API)
                        ws_av = _escribir_hoja(libro, "avances")
                        ws_av.append_rows(filas_sheets, value_input_option="USER_ENTERED")

                        # Actualizar cache local
                        for _, row in df_con_reporte.iterrows():
                            _cache_append("c_avances",{"proyecto":PA,"fecha":hoy_str,"id_item":row["ID"],"cantidad":row["Reportar hoy"],"usuario":u,"timestamp":ahora})

                        # Mensaje resumen en chat
                        items_txt = "<br>".join(resumen_items)
                        msg = f"<b>Avances registrados ({n_reportes}):</b><br>{items_txt}"
                        st.session_state.chat_historial[PA].append({"role":"assistant","content":msg})
                        st.session_state.acceso_rapido_modo = None
                        st.rerun()

        else: st.warning("No hay actividades en la hoja 'actividades'.")
        st.markdown("</div>", unsafe_allow_html=True)

    # ── MATERIAL ──
    elif modo=="material":
        st.markdown('<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#e65100;border-bottom:2px solid #ff6b35;padding-bottom:8px;margin-bottom:16px">Solicitar Material</div>', unsafe_allow_html=True)
        mats = ["Gravilla","Cemento","Varilla","Madera","Ladrillo","Acero","Agua","Formaleta","Arena","Puntillas"]
        ms = st.session_state.get("mqs","")
        cols = st.columns(5)
        for i,mat in enumerate(mats):
            with cols[i%5]:
                if st.button(mat,key=f"m{i}",use_container_width=True,type="primary" if ms==mat else "secondary"):
                    st.session_state["mqs"]="" if ms==mat else mat; st.rerun()
        req = st.text_area("Requerimiento:",value=ms,placeholder="ej: 50 bultos cemento…",height=80,key="mr")
        _,c2 = st.columns([3,1])
        with c2:
            if st.button("Enviar",use_container_width=True,key="me",type="primary",disabled=not req.strip()):
                m=f"Necesito {req.strip()}"; st.session_state.chat_historial[PA].append({"role":"user","content":m})
                st.session_state.chat_historial[PA].append({"role":"assistant","content":procesar_mensaje(m,PA,libro)})
                st.session_state.acceso_rapido_modo=None; st.session_state["mqs"]=""; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── ASISTENCIA ──
    elif modo=="asistencia":
        trabs = get_trabajadores()
        dah = get_asistencia(PA)
        if not dah.empty and "fecha" in dah.columns: dah = dah[dah["fecha"]==hoy_str]
        else: dah = pd.DataFrame()
        pr=int((dah["estado"]=="Presente").sum()) if not dah.empty else 0
        au=int((dah["estado"]=="Ausente").sum()) if not dah.empty else 0
        pm=int((dah["estado"]=="Permiso").sum()) if not dah.empty else 0
        st.markdown(f'<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#1565c0;border-bottom:2px solid #34b7f1;padding-bottom:8px;margin-bottom:16px">Asistencia — {hoy.strftime("%d/%m/%Y")}</div>', unsafe_allow_html=True)
        k1,k2,k3,k4 = st.columns(4)
        tp = len(trabs) if trabs else "—"
        for c,v,l,cl in [(k1,tp,"Plantilla","#34b7f1"),(k2,pr,"Presentes","#00a884"),(k3,au,"Ausentes","#e53935"),(k4,pm,"Permisos","#ff6b35")]:
            c.markdown(f'<div style="background:#f8f9fa;border-radius:8px;padding:10px 12px;text-align:center;border-top:3px solid {cl};margin-bottom:12px"><div style="font-size:22px;font-weight:700;color:#111b21">{v}</div><div style="font-size:11px;color:#667781">{l}</div></div>', unsafe_allow_html=True)
        if not trabs: st.warning("Agrega trabajadores en la hoja 'trabajadores'.")
        else:
            ya = dict(zip(dah["trabajador"],dah["estado"])) if not dah.empty else {}
            de = pd.DataFrame([{"Trabajador":t["nombre"],"Cargo":t["cargo"],"Estado":ya.get(t["nombre"],"Presente")} for t in trabs])
            dr = st.data_editor(de,column_config={"Trabajador":st.column_config.TextColumn("Trabajador",disabled=True,width="large"),"Cargo":st.column_config.TextColumn("Cargo",disabled=True,width="medium"),"Estado":st.column_config.SelectboxColumn("Estado",options=["Presente","Ausente","Permiso"],required=True,width="small")},hide_index=True,use_container_width=True,key="dae")
            if st.button("Guardar asistencia",type="primary",use_container_width=True,key="gas"):
                ah=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"); u=usr_nombre()
                nf=[[PA,hoy_str,r["Trabajador"],r["Cargo"],r["Estado"],u,ah] for _,r in dr.iterrows()]
                reemplazar_filas_dia(libro,"asistencia",PA,hoy_str,nf)
                _cache_replace_dia("c_asistencia",PA,hoy_str,[{"proyecto":PA,"fecha":hoy_str,"trabajador":r["Trabajador"],"cargo":r["Cargo"],"estado":r["Estado"],"usuario":u,"timestamp":ah} for _,r in dr.iterrows()])
                pn=dr[dr["Estado"]=="Presente"].shape[0]; an=dr[dr["Estado"]=="Ausente"].shape[0]; pmn=dr[dr["Estado"]=="Permiso"].shape[0]
                st.session_state.chat_historial[PA].append({"role":"assistant","content":f"<b>Asistencia guardada:</b> {pn} presentes, {an} ausentes, {pmn} permisos."})
                st.session_state.acceso_rapido_modo=None; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── NOMINA (Director) ──
    elif modo=="nomina" and es_director():
        st.markdown('<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#6a1b9a;border-bottom:2px solid #9c27b0;padding-bottom:8px;margin-bottom:16px">Nomina / Pagos</div>', unsafe_allow_html=True)
        trabs=get_trabajadores(); noms=[t["nombre"] for t in trabs] if trabs else []
        TIPOS=["Mensual","Quincenal","Jornal diario","Prestamo","Anticipo","Bonificacion","Deduccion","Liquidacion","Pago proveedor"]
        n1,n2=st.columns(2)
        with n1: ts=st.selectbox("Beneficiario:",noms,key="nt") if noms else st.text_input("Beneficiario:",key="ntt")
        with n2: tp=st.selectbox("Tipo:",TIPOS,key="ntp")
        cg="";
        if trabs and ts:
            for t in trabs:
                if t["nombre"]==ts: cg=t.get("cargo",""); break
        n3,n4=st.columns(2)
        with n3: val=st.number_input("Valor ($):",min_value=0.0,step=10000.0,format="%.0f",key="nv")
        with n4: con=st.text_input("Concepto:",key="nc")
        if ts:
            dfn=get_nomina(PA)
            if not dfn.empty and "trabajador" in dfn.columns:
                dfn2=dfn[dfn["trabajador"]==ts]
                if not dfn2.empty:
                    ing=dfn2[dfn2["tipo"].isin(["Mensual","Quincenal","Jornal diario","Bonificacion","Liquidacion"])]["valor"].sum()
                    pre=dfn2[dfn2["tipo"].isin(["Prestamo","Anticipo"])]["valor"].sum()
                    ded=dfn2[dfn2["tipo"]=="Deduccion"]["valor"].sum()
                    r1,r2,r3,r4=st.columns(4)
                    for c,v,l,cl in [(r1,fmt_cop(ing),"Pagos","#00a884"),(r2,fmt_cop(pre),"Prestamos","#ff6b35"),(r3,fmt_cop(ded),"Deducciones","#e53935"),(r4,fmt_cop(ing-pre-ded),"Neto","#34b7f1")]:
                        c.markdown(f'<div style="background:#f8f9fa;border-radius:8px;padding:8px 10px;text-align:center;border-top:3px solid {cl};margin-bottom:8px"><div style="font-size:15px;font-weight:700">{v}</div><div style="font-size:10px;color:#667781">{l}</div></div>', unsafe_allow_html=True)
        _,nr=st.columns([3,1])
        with nr:
            if st.button("Registrar pago",use_container_width=True,key="ng",type="primary",disabled=(val<=0 or not ts)):
                ah=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"); u=usr_nombre()
                agregar_fila(libro,"nomina",[PA,hoy_str,ts,cg,tp,con,val,u,ah])
                _cache_append("c_nomina",{"proyecto":PA,"fecha":hoy_str,"trabajador":ts,"cargo":cg,"tipo":tp,"concepto":con,"valor":val,"usuario":u,"timestamp":ah})
                st.session_state.chat_historial[PA].append({"role":"assistant","content":f"<b>Pago:</b> {fmt_cop(val)} — {tp} a <b>{ts}</b>. {con}"})
                st.session_state.acceso_rapido_modo=None; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── ASIGNAR TAREA (Director) ──
    elif modo=="tarea" and es_director():
        st.markdown('<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#1b5e20;border-bottom:2px solid #4caf50;padding-bottom:8px;margin-bottom:16px">Asignar Tarea</div>', unsafe_allow_html=True)
        trabs=get_trabajadores(); noms=[t["nombre"] for t in trabs] if trabs else []
        t1,t2=st.columns(2)
        with t1: desc=st.text_area("Descripcion:",height=80,key="td")
        with t2: asig=st.selectbox("Asignar a:",noms,key="ta") if noms else st.text_input("Asignar a:",key="tat")
        t3,t4,t5=st.columns(3)
        with t3: fl=st.date_input("Limite:",value=hoy+datetime.timedelta(days=1),key="tf")
        with t4: pri=st.selectbox("Prioridad:",["Alta","Media","Baja"],index=1,key="tp2")
        with t5: not_=st.text_input("Notas:",key="tn")
        _,tr=st.columns([3,1])
        with tr:
            if st.button("Asignar",use_container_width=True,key="tg",type="primary",disabled=(not desc.strip() or not asig)):
                ah=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"); tid=generar_id_tarea(libro,PA); u=usr_nombre()
                agregar_fila(libro,"tareas",[PA,tid,desc.strip(),asig,hoy_str,fl.strftime("%Y-%m-%d"),pri,"Pendiente",not_,u,ah])
                _cache_append("c_tareas",{"proyecto":PA,"id_tarea":tid,"descripcion":desc.strip(),"asignado_a":asig,"fecha_asignacion":hoy_str,"fecha_limite":fl.strftime("%Y-%m-%d"),"prioridad":pri,"estado":"Pendiente","notas":not_,"creado_por":u,"timestamp":ah})
                st.session_state.chat_historial[PA].append({"role":"assistant","content":f"<b>Tarea {tid} → {asig}:</b> {desc.strip()[:60]}… Limite: {fl.strftime('%d/%m/%Y')}"})
                st.session_state.acceso_rapido_modo=None; st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── MIS TAREAS (Residente) ──
    elif modo=="mis_tareas" and es_residente():
        st.markdown('<div style="background:#fff;border-radius:12px;border:1px solid #d1d7db;padding:20px 24px;margin:6px 0 10px;box-shadow:0 2px 8px rgba(0,0,0,.08)"><div style="font-size:15px;font-weight:600;color:#1b5e20;border-bottom:2px solid #4caf50;padding-bottom:8px;margin-bottom:16px">Mis Tareas</div>', unsafe_allow_html=True)
        dmt=get_tareas(PA)
        if not dmt.empty and "asignado_a" in dmt.columns: dmt=dmt[dmt["asignado_a"]==usr_nombre()]
        if dmt.empty: st.info("Sin tareas asignadas.")
        else:
            ct=[c for c in ["id_tarea","descripcion","fecha_limite","prioridad","estado","notas"] if c in dmt.columns]
            dte=st.data_editor(dmt[ct].copy(),column_config={"id_tarea":st.column_config.TextColumn("ID",disabled=True,width="small"),"descripcion":st.column_config.TextColumn("Tarea",disabled=True,width="large"),"fecha_limite":st.column_config.TextColumn("Limite",disabled=True,width="small"),"prioridad":st.column_config.TextColumn("Prior.",disabled=True,width="small"),"estado":st.column_config.SelectboxColumn("Estado",options=["Pendiente","En progreso","Completada"],required=True,width="medium"),"notas":st.column_config.TextColumn("Notas",width="medium")},hide_index=True,use_container_width=True,key="dmte")
            if st.button("Actualizar",type="primary",use_container_width=True,key="gmte"):
                ws=_escribir_hoja(libro,"tareas"); av=ws.get_all_values(); enc=[h.strip().lower().replace(" ","_") for h in av[0]]
                ie=enc.index("estado") if "estado" in enc else None; ino=enc.index("notas") if "notas" in enc else None; iid=enc.index("id_tarea") if "id_tarea" in enc else None
                if iid is not None and ie is not None:
                    for _,r in dte.iterrows():
                        for i,f in enumerate(av[1:],start=2):
                            if len(f)>iid and f[iid]==str(r["id_tarea"]):
                                if f[ie]!=r["estado"]: ws.update_cell(i,ie+1,r["estado"])
                                if ino and "notas" in r: ws.update_cell(i,ino+1,str(r.get("notas","")))
                                break
                # Actualizar cache local de tareas
                df_tar_cache = st.session_state.c_tareas
                if df_tar_cache is not None and not df_tar_cache.empty and "id_tarea" in df_tar_cache.columns:
                    for _,r in dte.iterrows():
                        mask = df_tar_cache["id_tarea"]==str(r["id_tarea"])
                        if mask.any():
                            df_tar_cache.loc[mask,"estado"] = r["estado"]
                            if "notas" in r: df_tar_cache.loc[mask,"notas"] = str(r.get("notas",""))
                    st.session_state.c_tareas = df_tar_cache
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if prompt := st.chat_input("Escribe una novedad..."):
        st.session_state.chat_historial[PA].append({"role":"user","content":prompt})
        st.session_state.chat_historial[PA].append({"role":"assistant","content":procesar_mensaje(prompt,PA,libro)})
        st.session_state.acceso_rapido_modo=None; st.rerun()


# ═══════════════════════════════════════════════════════════
# PANEL DEL DIRECTOR
# ═══════════════════════════════════════════════════════════
elif menu == "📊 Panel del Director":
    st.markdown('<div style="background:#075e54;padding:18px 28px;border-radius:12px;margin-bottom:24px;display:flex;align-items:center;gap:16px"><span style="font-size:32px">📊</span><div><div style="color:#fff;font-size:20px;font-weight:700">Panel de Control</div><div style="color:#b2dfdb;font-size:13px">Seguimiento en tiempo real</div></div></div>', unsafe_allow_html=True)

    PA = st.session_state.nombre_proyecto
    rf = st.session_state.resumen_fin
    df_act = get_actividades()
    if df_act.empty: st.info("No hay actividades."); st.stop()

    df_av = get_avances(PA)
    df_ca = calcular_avances(df_act, df_av)

    st.markdown(f'<div style="background:#e8f5e9;border-radius:8px;padding:10px 16px;margin-bottom:20px"><div style="font-size:13px;color:#667781">Proyecto activo</div><div style="font-size:15px;font-weight:600;color:#075e54">{PA}</div></div>', unsafe_allow_html=True)

    tab_av, tab_tar, tab_nom, tab_otr = st.tabs(["Avance de Obra","Tareas","Nomina / Pagos","Materiales y Asistencia"])

    # ═══ TAB AVANCE ═══
    with tab_av:
        st.markdown("<div class='section-header'>Resumen Financiero</div>", unsafe_allow_html=True)
        c1,c2,c3,c4 = st.columns(4)
        c1.markdown(f"<div class='kpi-card'><div class='kpi-valor'>{fmt_cop(rf.get('total_costo_directo',0))}</div><div class='kpi-label'>Costo Directo</div></div>", unsafe_allow_html=True)
        c2.markdown(f"<div class='kpi-card naranja'><div class='kpi-valor'>{fmt_cop(rf.get('total_costo_suministros',0))}</div><div class='kpi-label'>Suministros</div></div>", unsafe_allow_html=True)
        c3.markdown(f"<div class='kpi-card azul'><div class='kpi-valor'>{fmt_cop(rf.get('costo_total_de_la_obra',0))}</div><div class='kpi-label'>Costo Obra</div></div>", unsafe_allow_html=True)
        c4.markdown(f"<div class='kpi-card rojo'><div class='kpi-valor'>{fmt_cop(rf.get('costo_total_del_proyecto',0))}</div><div class='kpi-label'>Costo Proyecto</div></div>", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<div class='section-header'>Indicadores de Avance</div>", unsafe_allow_html=True)
        tot=len(df_ca); con_a=int((df_ca["cantidad_ejecutada"]>0).sum()); comp=int((df_ca["pct_avance"]>=100).sum())
        # Avance fisico ponderado por valor: sum(pct_i * valor_total_i) / sum(valor_total_i)
        v_ej=df_ca["valor_ejecutado"].sum()
        v_tot_act=df_ca["valor_total"].sum()
        # Usar costo directo de config como base, fallback a suma de actividades
        v_tot_base = rf.get("total_costo_directo", 0)
        if v_tot_base <= 0: v_tot_base = v_tot_act
        pct_g=round((df_ca["pct_avance"]*df_ca["valor_total"]).sum()/v_tot_act,1) if v_tot_act>0 else 0
        pct_f=round(v_ej/v_tot_base*100,1) if v_tot_base>0 else 0

        k1,k2,k3,k4,k5,k6 = st.columns(6)
        for col,val,lbl,clr in [(k1,tot,"Total","#00a884"),(k2,con_a,"Con Avance","#34b7f1"),(k3,comp,"Completadas","#25d366"),(k4,f"{fmt_dec(pct_g,1)}%","Fisico","#ff6b35"),(k5,fmt_cop(v_ej),"Ejecutado","#075e54"),(k6,f"{fmt_dec(pct_f,1)}%","Financiero","#e53935")]:
            col.markdown(f'<div style="background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-top:4px solid {clr};text-align:center;margin-bottom:8px"><div style="font-size:22px;font-weight:700;color:#111b21">{val}</div><div style="font-size:12px;color:#667781">{lbl}</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Mapa Presupuestal (Sunburst) — ancho completo ──
        st.markdown("<div class='section-header'>Mapa Presupuestal</div>", unsafe_allow_html=True)

        dft2=df_ca.groupby(["componente","capitulo"]).apply(lambda g: pd.Series({
            "vt":g["valor_total"].sum(),
            "pa":round((g["pct_avance"]*g["valor_total"]).sum()/g["valor_total"].sum(),1) if g["valor_total"].sum()>0 else 0
        })).reset_index()
        dft2["et"]=dft2["capitulo"].str[:25]
        comps = dft2["componente"].unique().tolist()

        # Calcular avance ponderado para cada componente
        avance_comp = {}
        for c in comps:
            dc = dft2[dft2["componente"]==c]
            vt_c = dc["vt"].sum()
            avance_comp[c] = round((dc["pa"]*dc["vt"]).sum()/vt_c, 1) if vt_c > 0 else 0

        # Calcular avance ponderado del proyecto total
        vt_total = dft2["vt"].sum()
        avance_proy = round((dft2["pa"]*dft2["vt"]).sum()/vt_total, 1) if vt_total > 0 else 0

        labels = ["Proyecto"] + comps + dft2["et"].tolist()
        parents = [""] + ["Proyecto"]*len(comps) + dft2["componente"].tolist()
        values = [vt_total] + [dft2[dft2["componente"]==c]["vt"].sum() for c in comps] + dft2["vt"].tolist()
        colors_vals = [avance_proy] + [avance_comp[c] for c in comps] + dft2["pa"].tolist()

        fig_sun = go.Figure(go.Sunburst(
            labels=labels, parents=parents, values=values,
            marker=dict(colors=colors_vals,
                colorscale=[[0,"#ffcdd2"],[0.3,"#fff9c4"],[0.6,"#c8e6c9"],[1,"#00a884"]],
                cmin=0,cmax=100,
                colorbar=dict(title="% Avance",thickness=14,len=0.7),
                line=dict(width=2,color="white")),
            branchvalues="total",
            hovertemplate="<b>%{label}</b><br>Valor: %{value:,.0f}<br>Avance: %{color:.1f}%<extra></extra>",
            texttemplate="<b>%{label}</b>",textfont=dict(size=10),
            insidetextorientation="radial"))
        fig_sun.update_layout(
            paper_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=20,b=10,l=10,r=10),height=500)
        st.plotly_chart(fig_sun,use_container_width=True)

        # ── Estado de actividades (Donut) + Ejecucion por componente (barras apiladas) ──
        st.markdown("<div class='section-header'>Analisis de Actividades</div>", unsafe_allow_html=True)
        gc1,gc2 = st.columns(2)

        with gc1:
            # Donut — estado global
            def est(p):
                if p==0: return "Sin Inicio"
                elif p<50: return "Inicio"
                elif p<100: return "En Progreso"
                return "Completado"
            df_ca["estado_avance"]=df_ca["pct_avance"].apply(est)
            conteo = df_ca["estado_avance"].value_counts().reset_index()
            conteo.columns = ["Estado","n"]
            col_map = {"Completado":"#00a884","En Progreso":"#ff6b35","Inicio":"#fdd835","Sin Inicio":"#e0e0e0"}
            fig_donut = go.Figure(go.Pie(
                labels=conteo["Estado"], values=conteo["n"], hole=0.55,
                marker=dict(colors=[col_map.get(e,"#ccc") for e in conteo["Estado"]],line=dict(color="white",width=2)),
                textinfo="label+value",textposition="inside",textfont=dict(size=11),
                hovertemplate="<b>%{label}</b><br>%{value} actividades (%{percent})<extra></extra>"))
            fig_donut.update_layout(
                title=dict(text="Estado de Actividades",font=dict(size=14)),
                paper_bgcolor="white",font=dict(family="Segoe UI",size=11),
                margin=dict(t=50,b=10,l=10,r=10),height=380,showlegend=True,
                legend=dict(orientation="h",y=-0.1,font=dict(size=10)),
                annotations=[dict(text=f"{tot}<br>total",x=0.5,y=0.5,font=dict(size=14,color="#075e54"),showarrow=False)])
            st.plotly_chart(fig_donut,use_container_width=True)

        with gc2:
            # Barras apiladas — ejecutado vs pendiente por componente
            dfc=df_ca.groupby("componente").agg(vt=("valor_total","sum"),ve=("valor_ejecutado","sum")).reset_index()
            dfc["vp"]=dfc["vt"]-dfc["ve"]; dfc["pct"]=(dfc["ve"]/dfc["vt"]*100).round(1).fillna(0)
            dfc = dfc.sort_values("vt",ascending=True)
            fig_ej = go.Figure()
            fig_ej.add_trace(go.Bar(name="Ejecutado",y=dfc["componente"],x=dfc["ve"],orientation="h",
                marker_color="#00a884",
                text=dfc.apply(lambda r: f"{fmt_dec(r['pct'],1)}%", axis=1),
                textposition="inside",textfont=dict(color="white",size=11)))
            fig_ej.add_trace(go.Bar(name="Pendiente",y=dfc["componente"],x=dfc["vp"],orientation="h",
                marker_color="#e8e8e8"))
            fig_ej.update_layout(barmode="stack",
                title=dict(text="Ejecucion por Componente",font=dict(size=14)),
                plot_bgcolor="white",paper_bgcolor="white",font=dict(family="Segoe UI",size=11),
                legend=dict(orientation="h",y=-0.12,font=dict(size=10)),
                margin=dict(t=45,b=30,l=10),height=380,
                xaxis=dict(showticklabels=False,showgrid=False))
            st.plotly_chart(fig_ej,use_container_width=True)

        st.markdown("<div class='section-header'>Tabla de Actividades</div>", unsafe_allow_html=True)
        tf1,tf2,tf3=st.columns(3)
        with tf1: fc_=st.multiselect("Componente:",df_ca["componente"].unique().tolist(),default=[],key="dfc")
        with tf2: fcp=st.multiselect("Capitulo:",df_ca["capitulo"].unique().tolist(),default=[],key="dfcp")
        with tf3: fes=st.multiselect("Estado:",["Sin Inicio","Inicio","En Progreso","Completado"],default=[],key="dfe")
        dtb=df_ca.copy()
        if fc_: dtb=dtb[dtb["componente"].isin(fc_)]
        if fcp: dtb=dtb[dtb["capitulo"].isin(fcp)]
        if fes: dtb=dtb[dtb["estado_avance"].isin(fes)]
        dm=dtb[["id","componente","capitulo","descripcion","unidad","valor_unitario","cantidad_total","cantidad_ejecutada","pct_avance","valor_total","valor_ejecutado","estado_avance"]].copy()
        dm.columns=["ID","Comp","Cap","Descripcion","Und","V.Unit","Cant.Tot","Cant.Ejec","% Av","V.Total","V.Ejec","Estado"]
        st.dataframe(dm.style.background_gradient(subset=["% Av"],cmap="Greens").format({"V.Unit":lambda x:fmt_cop(x),"V.Total":lambda x:fmt_cop(x),"V.Ejec":lambda x:fmt_cop(x),"% Av":"{:.1f}%","Cant.Tot":lambda x:fmt_dec(x),"Cant.Ejec":lambda x:fmt_dec(x)}),use_container_width=True,height=420,hide_index=True)

        st.markdown("<div class='section-header'>Resumen por Capitulo</div>", unsafe_allow_html=True)
        drc=df_ca.groupby(["componente_id","componente","capitulo_id","capitulo"]).apply(lambda g: pd.Series({"n":len(g),"vt":g["valor_total"].sum(),"ve":g["valor_ejecutado"].sum(),"pa":round((g["pct_avance"]*g["valor_total"]).sum()/g["valor_total"].sum(),1) if g["valor_total"].sum()>0 else 0})).reset_index()
        drc["vp"]=drc["vt"]-drc["ve"]; drc["pf"]=(drc["ve"]/drc["vt"]*100).round(1).fillna(0); drc=drc.sort_values(["componente_id","capitulo_id"])
        drm=drc[["componente","capitulo","n","vt","ve","vp","pa","pf"]].copy()
        drm.columns=["Comp","Cap","#","V.Total","V.Ejec","V.Pend","% Fis","% Fin"]
        st.dataframe(drm.style.background_gradient(subset=["% Fis","% Fin"],cmap="Greens").format({"V.Total":lambda x:fmt_cop(x),"V.Ejec":lambda x:fmt_cop(x),"V.Pend":lambda x:fmt_cop(x),"% Fis":"{:.1f}%","% Fin":"{:.1f}%"}),use_container_width=True,height=380,hide_index=True)

    # ═══ TAB TAREAS ═══
    with tab_tar:
        st.markdown("<div class='section-header'>Gestion de Tareas</div>", unsafe_allow_html=True)
        dtar=get_tareas(PA)
        if dtar.empty: st.info("Sin tareas.")
        else:
            tt=len(dtar); pe_=int((dtar["estado"]=="Pendiente").sum()) if "estado" in dtar.columns else 0
            pr_=int((dtar["estado"]=="En progreso").sum()) if "estado" in dtar.columns else 0
            co_=int((dtar["estado"]=="Completada").sum()) if "estado" in dtar.columns else 0
            ve_=0
            if "fecha_limite" in dtar.columns:
                dtar["fl"]=pd.to_datetime(dtar["fecha_limite"],errors="coerce")
                ve_=int(((dtar["fl"]<pd.Timestamp(hoy))&(dtar["estado"].isin(["Pendiente","En progreso"]))).sum())
            kt1,kt2,kt3,kt4,kt5=st.columns(5)
            for c,v,l,cl in [(kt1,tt,"Total","#34b7f1"),(kt2,pe_,"Pendientes","#ff6b35"),(kt3,pr_,"En progreso","#fdd835"),(kt4,co_,"Completadas","#00a884"),(kt5,ve_,"Vencidas","#e53935")]:
                c.markdown(f'<div style="background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-top:4px solid {cl};text-align:center;margin-bottom:8px"><div style="font-size:22px;font-weight:700">{v}</div><div style="font-size:12px;color:#667781">{l}</div></div>', unsafe_allow_html=True)
            ct=[c for c in ["id_tarea","descripcion","asignado_a","fecha_limite","prioridad","estado","notas"] if c in dtar.columns]
            dte=st.data_editor(dtar[ct].copy(),column_config={"id_tarea":st.column_config.TextColumn("ID",disabled=True,width="small"),"descripcion":st.column_config.TextColumn("Desc",disabled=True,width="large"),"asignado_a":st.column_config.TextColumn("Asignado",disabled=True,width="medium"),"fecha_limite":st.column_config.TextColumn("Limite",disabled=True,width="small"),"prioridad":st.column_config.TextColumn("Prior.",disabled=True,width="small"),"estado":st.column_config.SelectboxColumn("Estado",options=["Pendiente","En progreso","Completada","Cancelada"],required=True,width="medium"),"notas":st.column_config.TextColumn("Notas",width="medium")},hide_index=True,use_container_width=True,key="dted")
            if st.button("Guardar tareas",type="primary",key="gtd"):
                ws=_escribir_hoja(libro,"tareas"); av=ws.get_all_values(); enc=[h.strip().lower().replace(" ","_") for h in av[0]]
                ie=enc.index("estado") if "estado" in enc else None; ino=enc.index("notas") if "notas" in enc else None; iid=enc.index("id_tarea") if "id_tarea" in enc else None
                if iid is not None and ie is not None:
                    for _,r in dte.iterrows():
                        for i,f in enumerate(av[1:],start=2):
                            if len(f)>iid and f[iid]==str(r["id_tarea"]):
                                if f[ie]!=r["estado"]: ws.update_cell(i,ie+1,r["estado"])
                                if ino and "notas" in r: ws.update_cell(i,ino+1,str(r.get("notas","")))
                                break
                df_tar_cache = st.session_state.c_tareas
                if df_tar_cache is not None and not df_tar_cache.empty and "id_tarea" in df_tar_cache.columns:
                    for _,r in dte.iterrows():
                        mask = df_tar_cache["id_tarea"]==str(r["id_tarea"])
                        if mask.any():
                            df_tar_cache.loc[mask,"estado"] = r["estado"]
                            if "notas" in r: df_tar_cache.loc[mask,"notas"] = str(r.get("notas",""))
                    st.session_state.c_tareas = df_tar_cache
                st.rerun()
            with gc1:
                if "estado" in dtar.columns:
                    ce=dtar["estado"].value_counts().reset_index(); ce.columns=["E","N"]
                    f=px.pie(ce,names="E",values="N",title="Por Estado",hole=.4,color="E",color_discrete_map={"Pendiente":"#ff6b35","En progreso":"#fdd835","Completada":"#00a884","Cancelada":"#e0e0e0"})
                    f.update_layout(paper_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=300)
                    st.plotly_chart(f,use_container_width=True)
            with gc2:
                if "asignado_a" in dtar.columns:
                    da=dtar.groupby(["asignado_a","estado"]).size().reset_index(name="n")
                    f=px.bar(da,x="asignado_a",y="n",color="estado",title="Tareas por Persona",barmode="stack",color_discrete_map={"Pendiente":"#ff6b35","En progreso":"#fdd835","Completada":"#00a884","Cancelada":"#e0e0e0"})
                    f.update_layout(paper_bgcolor="white",plot_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=300)
                    st.plotly_chart(f,use_container_width=True)

    # ═══ TAB NOMINA ═══
    with tab_nom:
        st.markdown("<div class='section-header'>Nomina y Pagos</div>", unsafe_allow_html=True)

        # ── Formulario para registrar pago ──
        with st.expander("Registrar nuevo pago", expanded=False):
            trabs_nom = get_trabajadores()
            noms_nom = [t["nombre"] for t in trabs_nom] if trabs_nom else []
            TIPOS_NOM = ["Mensual","Quincenal","Jornal diario","Prestamo","Anticipo","Bonificacion","Deduccion","Liquidacion","Pago proveedor"]

            fn1,fn2 = st.columns(2)
            with fn1:
                nom_trab = st.selectbox("Trabajador / Beneficiario:", noms_nom, key="dir_nom_trab") if noms_nom else st.text_input("Beneficiario:", key="dir_nom_trab_txt")
            with fn2:
                nom_tipo = st.selectbox("Tipo de movimiento:", TIPOS_NOM, key="dir_nom_tipo")

            nom_cargo = ""
            if trabs_nom and nom_trab:
                for t in trabs_nom:
                    if t["nombre"] == nom_trab: nom_cargo = t.get("cargo",""); break

            fn3,fn4 = st.columns(2)
            with fn3:
                nom_valor = st.number_input("Valor ($):", min_value=0.0, step=10000.0, format="%.0f", key="dir_nom_val")
            with fn4:
                nom_concepto = st.text_input("Concepto / Detalle:", placeholder="ej: Quincena 1-15 marzo", key="dir_nom_con")

            # Resumen del trabajador
            if nom_trab:
                dfn_t = get_nomina(PA)
                if not dfn_t.empty and "trabajador" in dfn_t.columns:
                    dfn_t2 = dfn_t[dfn_t["trabajador"]==nom_trab]
                    if not dfn_t2.empty:
                        ing=dfn_t2[dfn_t2["tipo"].isin(["Mensual","Quincenal","Jornal diario","Bonificacion","Liquidacion"])]["valor"].sum()
                        pre=dfn_t2[dfn_t2["tipo"].isin(["Prestamo","Anticipo"])]["valor"].sum()
                        ded=dfn_t2[dfn_t2["tipo"]=="Deduccion"]["valor"].sum()
                        rn1,rn2,rn3,rn4 = st.columns(4)
                        for c,v,l,cl in [(rn1,fmt_cop(ing),"Pagos","#00a884"),(rn2,fmt_cop(pre),"Prestamos","#ff6b35"),(rn3,fmt_cop(ded),"Deducciones","#e53935"),(rn4,fmt_cop(ing-pre-ded),"Neto","#34b7f1")]:
                            c.markdown(f'<div style="background:#f8f9fa;border-radius:8px;padding:8px 10px;text-align:center;border-top:3px solid {cl};margin-bottom:8px"><div style="font-size:15px;font-weight:700">{v}</div><div style="font-size:10px;color:#667781">{l}</div></div>', unsafe_allow_html=True)

            _,btn_col = st.columns([3,1])
            with btn_col:
                if st.button("Registrar pago", use_container_width=True, key="dir_nom_guardar", type="primary",
                             disabled=(nom_valor <= 0 or not nom_trab)):
                    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    u = usr_nombre()
                    agregar_fila(libro, "nomina", [PA, hoy_str, nom_trab, nom_cargo, nom_tipo, nom_concepto, nom_valor, u, ahora])
                    _cache_append("c_nomina",{"proyecto":PA,"fecha":hoy_str,"trabajador":nom_trab,"cargo":nom_cargo,"tipo":nom_tipo,"concepto":nom_concepto,"valor":nom_valor,"usuario":u,"timestamp":ahora})
                    st.success(f"Pago registrado: {fmt_cop(nom_valor)} — {nom_tipo} a {nom_trab}")
                    st.rerun()

        # ── Dashboard de nómina ──
        dfn=get_nomina(PA)
        if dfn.empty: st.info("Sin movimientos.")
        else:
            tpa=dfn[dfn["tipo"].isin(["Mensual","Quincenal","Jornal diario","Bonificacion","Liquidacion"])]["valor"].sum()
            tpr=dfn[dfn["tipo"].isin(["Prestamo","Anticipo"])]["valor"].sum()
            tde=dfn[dfn["tipo"]=="Deduccion"]["valor"].sum()
            tpv=dfn[dfn["tipo"]=="Pago proveedor"]["valor"].sum()
            kn1,kn2,kn3,kn4,kn5=st.columns(5)
            for c,v,l,cl in [(kn1,fmt_cop(tpa),"Pagos","#00a884"),(kn2,fmt_cop(tpr),"Prestamos","#ff6b35"),(kn3,fmt_cop(tde),"Deducciones","#e53935"),(kn4,fmt_cop(tpa-tde),"Neto","#34b7f1"),(kn5,fmt_cop(tpv),"Proveedores","#9c27b0")]:
                c.markdown(f'<div style="background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-top:4px solid {cl};text-align:center;margin-bottom:8px"><div style="font-size:20px;font-weight:700">{v}</div><div style="font-size:12px;color:#667781">{l}</div></div>', unsafe_allow_html=True)
            gn1,gn2=st.columns(2)
            with gn1:
                dt=dfn.groupby("tipo")["valor"].sum().reset_index()
                f=px.bar(dt,x="tipo",y="valor",title="Por Tipo",color="tipo",color_discrete_sequence=COLORES)
                f.update_layout(paper_bgcolor="white",plot_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=320,showlegend=False,yaxis=dict(tickformat=",.0f"))
                st.plotly_chart(f,use_container_width=True)
            with gn2:
                if "trabajador" in dfn.columns:
                    dnt=dfn.groupby("trabajador")["valor"].sum().reset_index().sort_values("valor",ascending=False).head(10)
                    f=px.bar(dnt,x="valor",y="trabajador",orientation="h",title="Top 10",color="valor",color_continuous_scale=["#e8f5e9","#00a884","#075e54"])
                    f.update_layout(paper_bgcolor="white",plot_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=320,coloraxis_showscale=False,xaxis=dict(tickformat=",.0f"))
                    st.plotly_chart(f,use_container_width=True)
            cn=[c for c in ["fecha","trabajador","cargo","tipo","concepto","valor","usuario"] if c in dfn.columns]
            st.dataframe(dfn[cn].sort_values("fecha",ascending=False),hide_index=True,use_container_width=True,height=400)

    # ═══ TAB MATERIALES Y ASISTENCIA ═══
    with tab_otr:
        st.markdown("<div class='section-header'>Materiales</div>", unsafe_allow_html=True)
        dfm=get_materiales(PA)
        if not dfm.empty:
            cm=[c for c in ["fecha","requerimiento","estado","usuario"] if c in dfm.columns]
            st.dataframe(dfm[cm],hide_index=True,use_container_width=True)
        else: st.info("Sin solicitudes.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<div class='section-header'>Asistencia</div>", unsafe_allow_html=True)
        dfa=get_asistencia(PA)
        if dfa.empty: st.info("Sin registros.")
        else:
            # KPIs del día actual
            dfa_hoy = dfa[dfa["fecha"]==hoy_str] if "fecha" in dfa.columns else pd.DataFrame()
            pres_hoy = int((dfa_hoy["estado"]=="Presente").sum()) if not dfa_hoy.empty and "estado" in dfa_hoy.columns else 0
            aus_hoy = int((dfa_hoy["estado"]=="Ausente").sum()) if not dfa_hoy.empty and "estado" in dfa_hoy.columns else 0
            perm_hoy = int((dfa_hoy["estado"]=="Permiso").sum()) if not dfa_hoy.empty and "estado" in dfa_hoy.columns else 0
            total_hoy = pres_hoy + aus_hoy + perm_hoy

            st.markdown(f"<div style='font-size:13px;color:#667781;margin-bottom:10px'>Asistencia de hoy <b>{hoy.strftime('%d/%m/%Y')}</b></div>", unsafe_allow_html=True)
            ah1,ah2,ah3,ah4 = st.columns(4)
            for c,v,l,cl in [(ah1,total_hoy,"Registrados","#34b7f1"),(ah2,pres_hoy,"Presentes","#00a884"),(ah3,aus_hoy,"Ausentes","#e53935"),(ah4,perm_hoy,"Permisos","#ff6b35")]:
                c.markdown(f'<div style="background:#fff;border-radius:10px;padding:12px 14px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-top:4px solid {cl};text-align:center;margin-bottom:8px"><div style="font-size:22px;font-weight:700">{v}</div><div style="font-size:11px;color:#667781">{l}</div></div>', unsafe_allow_html=True)

            # Tabla del día actual
            if not dfa_hoy.empty:
                ca_hoy = [c for c in ["trabajador","cargo","estado"] if c in dfa_hoy.columns]
                st.dataframe(dfa_hoy[ca_hoy],hide_index=True,use_container_width=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Gráficos historicos
            d1,d2=st.columns(2)
            with d1:
                ce=dfa["estado"].value_counts().reset_index(); ce.columns=["E","N"]
                f=px.pie(ce,names="E",values="N",title="Asistencia Acumulada",color="E",color_discrete_map={"Presente":"#00a884","Ausente":"#e53935","Permiso":"#ff6b35"},hole=.4)
                f.update_layout(paper_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=300)
                st.plotly_chart(f,use_container_width=True)
            with d2:
                df2=dfa.copy(); df2["fecha"]=pd.to_datetime(df2["fecha"],errors="coerce")
                dg=df2.groupby(["fecha","estado"]).size().reset_index(name="n")
                f=px.line(dg,x="fecha",y="n",color="estado",title="Asistencia Diaria",color_discrete_map={"Presente":"#00a884","Ausente":"#e53935","Permiso":"#ff6b35"},markers=True)
                f.update_layout(paper_bgcolor="white",plot_bgcolor="white",font=dict(family="Segoe UI",size=11),margin=dict(t=40,b=20),height=300)
                st.plotly_chart(f,use_container_width=True)

            # Tabla historica completa
            st.markdown("<div style='font-size:13px;color:#667781;margin:8px 0'>Historial completo</div>", unsafe_allow_html=True)
            ca=[c for c in ["fecha","trabajador","cargo","estado","usuario"] if c in dfa.columns]
            st.dataframe(dfa[ca].sort_values("fecha",ascending=False),hide_index=True,use_container_width=True,height=350)


# ═══════════════════════════════════════════════════════════
# MIS TAREAS (Residente)
# ═══════════════════════════════════════════════════════════
elif menu == "📋 Mis Tareas":
    PA=st.session_state.nombre_proyecto; un=usr_nombre()
    st.markdown(f'<div style="background:#1b5e20;padding:18px 28px;border-radius:12px;margin-bottom:24px;display:flex;align-items:center;gap:16px"><span style="font-size:32px">📋</span><div><div style="color:#fff;font-size:20px;font-weight:700">Mis Tareas</div><div style="color:#a5d6a7;font-size:13px">Asignadas a {un}</div></div></div>', unsafe_allow_html=True)
    dtar=get_tareas(PA)
    if not dtar.empty and "asignado_a" in dtar.columns: dtar=dtar[dtar["asignado_a"]==un]
    if dtar.empty: st.info("Sin tareas asignadas.")
    else:
        pe_=int((dtar["estado"]=="Pendiente").sum()); pr_=int((dtar["estado"]=="En progreso").sum()); co_=int((dtar["estado"]=="Completada").sum())
        k1,k2,k3=st.columns(3)
        for c,v,l,cl in [(k1,pe_,"Pendientes","#ff6b35"),(k2,pr_,"En progreso","#fdd835"),(k3,co_,"Completadas","#00a884")]:
            c.markdown(f'<div style="background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.1);border-top:4px solid {cl};text-align:center;margin-bottom:8px"><div style="font-size:22px;font-weight:700">{v}</div><div style="font-size:12px;color:#667781">{l}</div></div>', unsafe_allow_html=True)
        ct=[c for c in ["id_tarea","descripcion","fecha_limite","prioridad","estado","notas","creado_por"] if c in dtar.columns]
        dte=st.data_editor(dtar[ct].copy(),column_config={"id_tarea":st.column_config.TextColumn("ID",disabled=True,width="small"),"descripcion":st.column_config.TextColumn("Tarea",disabled=True,width="large"),"fecha_limite":st.column_config.TextColumn("Limite",disabled=True,width="small"),"prioridad":st.column_config.TextColumn("Prior.",disabled=True,width="small"),"estado":st.column_config.SelectboxColumn("Estado",options=["Pendiente","En progreso","Completada"],required=True,width="medium"),"notas":st.column_config.TextColumn("Notas",width="medium"),"creado_por":st.column_config.TextColumn("Asignado por",disabled=True,width="small")},hide_index=True,use_container_width=True,key="pmte")
        if st.button("Guardar",type="primary",use_container_width=True,key="pgmte"):
            ws=_escribir_hoja(libro,"tareas"); av=ws.get_all_values(); enc=[h.strip().lower().replace(" ","_") for h in av[0]]
            ie=enc.index("estado") if "estado" in enc else None; ino=enc.index("notas") if "notas" in enc else None; iid=enc.index("id_tarea") if "id_tarea" in enc else None
            if iid is not None and ie is not None:
                for _,r in dte.iterrows():
                    for i,f in enumerate(av[1:],start=2):
                        if len(f)>iid and f[iid]==str(r["id_tarea"]):
                            if f[ie]!=r["estado"]: ws.update_cell(i,ie+1,r["estado"])
                            if ino and "notas" in r: ws.update_cell(i,ino+1,str(r.get("notas","")))
                            break
            df_tar_cache = st.session_state.c_tareas
            if df_tar_cache is not None and not df_tar_cache.empty and "id_tarea" in df_tar_cache.columns:
                for _,r in dte.iterrows():
                    mask = df_tar_cache["id_tarea"]==str(r["id_tarea"])
                    if mask.any():
                        df_tar_cache.loc[mask,"estado"] = r["estado"]
                        if "notas" in r: df_tar_cache.loc[mask,"notas"] = str(r.get("notas",""))
                st.session_state.c_tareas = df_tar_cache
            st.rerun()


# ═══════════════════════════════════════════════════════════
# PROVEEDORES (Director)
# ═══════════════════════════════════════════════════════════
elif menu == "🏢 Proveedores":
    st.markdown('<div style="background:#6a1b9a;padding:18px 28px;border-radius:12px;margin-bottom:24px;display:flex;align-items:center;gap:16px"><span style="font-size:32px">🏢</span><div><div style="color:#fff;font-size:20px;font-weight:700">Proveedores</div><div style="color:#ce93d8;font-size:13px">Directorio del proyecto</div></div></div>', unsafe_allow_html=True)
    dfp=_leer(libro,"proveedores")
    if not dfp.empty: dfp=_norm(dfp)
    with st.expander("Agregar proveedor",expanded=False):
        p1,p2=st.columns(2)
        with p1: pn=st.text_input("NIT:",key="pn"); pno=st.text_input("Nombre:",key="pno"); pco=st.text_input("Contacto:",key="pco")
        with p2:
            pte=st.text_input("Telefono:",key="pte"); pca=st.selectbox("Categoria:",["Materiales","Equipos","Transporte","Mano de obra","Servicios","Ferreteria","Concreto","Acero","Otro"],key="pca"); pdi=st.text_input("Direccion:",key="pdi")
        pnt=st.text_input("Notas:",key="pnt")
        if st.button("Guardar",type="primary",key="pg",disabled=(not pn.strip() or not pno.strip())):
            agregar_fila(libro,"proveedores",[pn.strip(),pno.strip(),pco.strip(),pte.strip(),pca,pdi.strip(),pnt.strip()])
            st.success(f"'{pno}' guardado."); st.rerun()
    if not dfp.empty:
        if "categoria" in dfp.columns:
            cats=["Todas"]+sorted(dfp["categoria"].dropna().unique().tolist())
            fc=st.selectbox("Filtrar:",cats,key="pfc")
            if fc!="Todas": dfp=dfp[dfp["categoria"]==fc]
        st.dataframe(dfp,hide_index=True,use_container_width=True,height=400)
    else: st.info("Sin proveedores registrados.")
