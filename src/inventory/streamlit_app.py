import re
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))
print("Added to sys.path:", pathlib.Path(__file__).parent.parent.resolve())

import os
import streamlit as st
import tempfile

from inventory.process_orders_master import run_process_orders
from inventory.undo_update import undo_inventory_update
from inventory.update_items import update_items_from_purchases_table




def cleanup_temp_files():
    # Cleanup temp files on reload
    if 'temp_files' in st.session_state:
        for fpath in st.session_state['temp_files']:
            try:
                os.remove(fpath)
            except Exception:
                pass
        del st.session_state['temp_files']


def run_parser_from_st(delivery_date, temp_paths):
    # Run parser
    st.info("Running...")
    sharepoint_paths = run_process_orders(delivery_date.strftime("%m/%d/%Y"), temp_paths)
    st.success("Success! Files saved in SharePoint")
    st.markdown(f"- 📂 `{sharepoint_paths}`")
    st.session_state['temp_files'] = temp_paths


def save_temp_files(uploaded_files):
    temp_paths = []
    if uploaded_files:
        for file in uploaded_files:
            ext = file.name.rsplit('.', 1)[-1].lower()
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)
    return temp_paths


def process_orders_section(delivery_date):
    st.subheader("🚚 Process Orders")

    # Upload Excel files
    uploaded_files = st.file_uploader(
        "Upload files",
        type=["xlsx", "csv"],
        accept_multiple_files=True,
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        start_clicked = st.button("Start", type="primary")
    with col2:
        st.checkbox(
            "Ignore processed POs",
            key="ignore_processed",
            value=False
        )

    # Run
    if start_clicked:
        if not uploaded_files:
            st.error("Upload a file first")
            st.stop()
        if not delivery_date:
            st.error("Enter delivery date")
            st.stop()

        temp_paths = save_temp_files(uploaded_files)
        run_parser_from_st(delivery_date, temp_paths)


def update_items_section(delivery_date):
    with st.expander("📝 Update Items from SharePoint"):
        update_table_name = st.text_input(
            "Table Name",
            placeholder="ex. S26",
            key="update_table_name"
        )

        if st.button("Update", key="update_button"):
            if not update_table_name or not update_table_name.strip():
                st.error("Enter table name")
                st.stop()
            if not delivery_date:
                st.error("Enter delivery date")
                st.stop()

            st.info("Updating items from Cloud...")
            try:
                files_save_path = update_items_from_purchases_table(
                    update_table_name.strip(),
                    delivery_date.strftime("%m/%d/%Y")
                )
                st.success("Update completed successfully!")
                if files_save_path:
                    st.markdown(f"- 📂 Files saved: `{files_save_path}`")
            except Exception as e:
                st.error(f"Update failed: {str(e)}")


def undo_section():
    with st.expander("↩️ Undo Actions"):
        recovery_id = st.text_input("Log ID action to undo - leave empty for last action", key="recovery_id_input")

        if st.button("Undo", key="undo_button"):
            try:
                recovery_id_int = int(recovery_id) if recovery_id.strip() else None
            except ValueError:
                st.error("Please enter a valid number.")
            else:
                reversed_actions = undo_inventory_update(recovery_id_int)
                st.success("Done. The following actions have been reversed")
                st.dataframe(
                    reversed_actions,
                    use_container_width=True,
                    hide_index=True,
                    column_config={"LOG_ID": st.column_config.NumberColumn(format="%.0f")}  # no commas, no decimals
                )


def main():
    st.title("📦 INVOC")

    # Shared parameter
    delivery_date = st.date_input("Delivery Date", help="Delivery date for orders")

    # Main action: Process Orders (most used)
    process_orders_section(delivery_date)

    st.divider()

    # Secondary actions (less frequently used)
    col1, col2 = st.columns(2)
    with col1:
        update_items_section(delivery_date)
    with col2:
        undo_section()

    cleanup_temp_files()


main()




