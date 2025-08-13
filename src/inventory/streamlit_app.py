import re
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))
print("Added to sys.path:", pathlib.Path(__file__).parent.parent.resolve())

import os
import streamlit as st
import tempfile

from inventory.po_parser import run_po_parser
from inventory.undo_update import undo_inventory_update


def validate_rfid_series(rfid_series_str: str) -> bool:
    if not rfid_series_str.strip():
        return True  # empty allowed

    ranges = [r.strip() for r in rfid_series_str.split(',')]

    prefix = None
    prev_end_num = None

    for r in ranges:
        # Validate format: prefix + digits - prefix + digits
        m = re.fullmatch(r'(C\d{8}|SB\d{7})-(C\d{8}|SB\d{7})', r)
        if not m:
            return False

        start, end = r.split('-')

        # On first range, save prefix
        if prefix is None:
            if start.startswith('C'):
                prefix = 'C'
            elif start.startswith('SB'):
                prefix = 'SB'
            else:
                return False
        # All subsequent ranges must have the same prefix
        if not start.startswith(prefix) or not end.startswith(prefix):
            return False

        # Remove prefix to get numeric parts
        start_num = int(start[len(prefix):])
        end_num = int(end[len(prefix):])

        # Check start <= end for each range
        if start_num > end_num:
            return False

        # Check strictly increasing ranges: start > previous range's end
        if prev_end_num is not None and start_num <= prev_end_num:
            return False

        prev_end_num = end_num

    return True


def cleanup_temp_files():
    # Cleanup temp files on reload
    if 'temp_files' in st.session_state:
        for fpath in st.session_state['temp_files']:
            try:
                os.remove(fpath)
            except Exception:
                pass
        del st.session_state['temp_files']


def run_parser_from_st(delivery_date, temp_paths, update_from_sharepoint):
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


def save_temp_files(uploaded_files):
    temp_paths = []
    if uploaded_files:
        for file in uploaded_files:
            ext = file.rsplit('.', 1)[-1].lower()
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)
    return temp_paths


def uploader_and_parameters():
    # Upload Excel files
    uploaded_files = st.file_uploader(
        "Upload files",
        type=["xlsx", "csv"],
        accept_multiple_files=True
    )
    # Optional: use existing file from SharePoint
    with st.expander("🔄 Update from SharePoint"):
        update_from_sharepoint = st.text_input(
            "Season",
            placeholder="ex. B26"
        )
    # Parameters
    delivery_date = st.date_input("Delivery Date")
    return delivery_date, update_from_sharepoint, uploaded_files


def undo_button():
    # --- UI ---
    st.title("📦 INVOC")
    # --- Undo (top-right, optional recovery_id) ---
    col1, col2 = st.columns([5, 1])
    with col2:
        with st.popover("↩️ undo"):
            recovery_id = st.text_input("log id (optional)", key="recovery_id_input")
            if st.button("Undo", key="undo_button"):
                try:
                    recovery_id_int = int(recovery_id) if recovery_id.strip() else None
                except ValueError:
                    st.error("Please enter a valid number.")
                else:
                    reversed_actions = undo_inventory_update(recovery_id_int)
                    st.success("Done. The following actions have been reversed")
                    st.table(reversed_actions)


def main():
    undo_button()
    delivery_date, update_from_sharepoint, uploaded_files = uploader_and_parameters()
    # Run
    if st.button("Start"):
        if not uploaded_files and not update_from_sharepoint:
            st.error("Upload a file first")
            st.stop()
        if not delivery_date:
            st.error("Enter delivery date")
            st.stop()

        temp_paths = save_temp_files(uploaded_files)

        run_parser_from_st(delivery_date, temp_paths, update_from_sharepoint)
    cleanup_temp_files()


main()




