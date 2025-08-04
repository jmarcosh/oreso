import streamlit as st
import tempfile
import os

from src.inventory.po_parser import run_po_parser

# --- UI ---
st.title("📦 INVOC")

# Upload Excel files
st.header("1. Sube la(s) Orden(es) de Compra")
uploaded_files = st.file_uploader("Archivo de Excel Original", type="xlsx", accept_multiple_files=True)

# Parameters
st.header("2. Ingresa Parametros")
delivery_date = st.date_input("Fecha de Entrega")
rfid_series_str = st.text_input("RFID Series (e.g., C52767864-C52768000,C56916036-C56917000)")

# Run
if st.button("Iniciar proceso"):
    if not uploaded_files:
        st.error("Sube al menos un archivo .xlsx")
    elif not delivery_date:
        st.error("Ingresa la fecha de entrega.")
    else:
        if rfid_series_str.strip():
            rfid_series = [x.strip().split('-') for x in rfid_series_str.split(',')]
        else:
            rfid_series = None

        # Save uploaded files to temp files and get paths
        temp_paths = []
        for file in uploaded_files:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            print("File saved at:", temp_file.name)
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)

        # Run parser
        st.info("Procesando archivo...")
        sharepoint_paths = run_po_parser(
            delivery_date.strftime("%m/%d/%Y"), rfid_series,
            temp_paths
        )
        st.success("¡Listo! Archivos guardados en SharePoint")

        # st.header("3. Download Files")
        # for filename, filepath in output_files.items():
        #     with open(filepath, 'rb') as f:
        #         st.download_button(label=f"Download {filename}", data=f, file_name=filename)
        st.markdown(f"- 📂 `{sharepoint_paths}`")

        # Track files for cleanup
        st.session_state['temp_files'] = temp_paths #+ list(output_files.values())

# Cleanup temp files on reload
if 'temp_files' in st.session_state:
    for fpath in st.session_state['temp_files']:
        try:
            os.remove(fpath)
        except Exception:
            pass
    del st.session_state['temp_files']
