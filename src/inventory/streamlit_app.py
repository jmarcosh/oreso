import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))
print("Added to sys.path:", pathlib.Path(__file__).parent.parent.resolve())

import os
import streamlit as st
import tempfile

from inventory.po_parser import run_po_parser
from inventory.undo_update import undo_update

# --- UI ---
st.title("📦 INVOC")

# --- Undo (top-right, optional recovery_id) ---
col1, col2 = st.columns([5, 1])
with col2:
    with st.popover("↩️ undo"):
        recovery_id = st.text_input("log id (optional)", key="recovery_id_input")
        if st.button("Undo", key="undo_button"):
            undo_update(recovery_id if recovery_id else None)
            st.success("Done")

# Upload Excel files
uploaded_files = st.file_uploader("Upload files", type="csv", accept_multiple_files=True)

# Optional: use existing file from SharePoint
with st.expander("🔄 Update from SharePoint"):
    update_from_sharepoint = st.text_input(
        "Season",
        placeholder="ex. B26"
    )

# Parameters
delivery_date = st.date_input("Delivery Date")

# Run
if st.button("Start"):
    if not uploaded_files and not update_from_sharepoint:
        st.error("Upload a file first")
        st.stop()
    if not delivery_date:
        st.error("Enter delivery date")
        st.stop()



    temp_paths = []
    if uploaded_files:
        for file in uploaded_files:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)

    # Run parser
    st.info("Running...")
    sharepoint_paths = run_po_parser(
        delivery_date.strftime("%m/%d/%Y"),
        temp_paths,
        update_from_sharepoint if update_from_sharepoint.strip() else None
    )
    st.success("Success! Files save in Sharepoint")
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




