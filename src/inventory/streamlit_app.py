import streamlit as st
import tempfile
import os

from src.inventory.common_app import validate_rfid_series
from src.inventory.po_parser import run_po_parser
from src.inventory.undo_update import undo_update

# --- UI ---
st.title("📦 INVOC")

# --- Undo (top-right, optional recovery_id) ---
col1, col2 = st.columns([5, 1])
with col2:
    with st.popover("↩️ undo"):
        recovery_id = st.text_input("ID de recuperación (opcional)", key="recovery_id_input")
        if st.button("Deshacer", key="undo_button"):
            undo_update(recovery_id if recovery_id else None)
            st.success("Se deshicieron los cambios.")

# Upload Excel files
st.header("1. Sube la(s) Orden(es) de Compra")
uploaded_files = st.file_uploader("Archivo de Excel Original", type="xlsx", accept_multiple_files=True)

# Optional: use existing file from SharePoint
with st.expander("🔄 O actualiza el inventario usando el catalogo"):
    update_from_sharepoint = st.text_input(
        "Temporada de recibo",
        placeholder="ej. B26"
    )

# Parameters
st.header("2. Ingresa Parametros")
delivery_date = st.date_input("Fecha de Entrega")
rfid_series_str = st.text_input("RFID Series (e.j., C52767864-C52768000,C56916036-C56917000)")

# Run
if st.button("Iniciar proceso"):
    if not uploaded_files and not update_from_sharepoint:
        st.error("Sube al menos un archivo .xlsx o indica el archivo en SharePoint.")
        st.stop()
    if not delivery_date:
        st.error("Ingresa la fecha de entrega.")
        st.stop()
    if not validate_rfid_series(rfid_series_str):
        st.error(
            "Formato de RFID inválido. Todos los rangos deben comenzar con el mismo prefijo (C o SB), "
            "los valores numéricos deben ser crecientes en cada rango."
        )
        st.stop()

    rfid_series = [x.strip().split('-') for x in rfid_series_str.split(',')] if rfid_series_str.strip() else None

    temp_paths = []
    if uploaded_files:
        for file in uploaded_files:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)

    # Run parser
    st.info("Procesando archivo...")
    sharepoint_paths = run_po_parser(
        delivery_date.strftime("%m/%d/%Y"),
        rfid_series,
        temp_paths,
        update_from_sharepoint if update_from_sharepoint.strip() else None
    )
    st.success("¡Listo! Archivos guardados en SharePoint")
    st.markdown(f"- 📂 `{sharepoint_paths}`")

    st.session_state['temp_files'] = temp_paths

# Cleanup temp files on reload
if 'temp_files' in st.session_state:
    for fpath in st.session_state['temp_files']:
        try:
            os.remove(fpath)
        except Exception:
            pass
    del st.session_state['temp_files']




