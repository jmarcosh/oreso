import streamlit as st
import pandas as pd
import tempfile
import os

# --- Your real function using paths ---
def run_po_parser(delivery_date, rfid_series, input_paths, config_path1, config_path2):
    # Read the files inside the function
    input_dfs = [pd.read_excel(p) for p in input_paths]
    config_df1 = pd.read_excel(config_path1)
    config_df2 = pd.read_excel(config_path2)

    outputs = {}
    for i, df in enumerate(input_dfs):
        result_df = df.copy()
        result_df['Delivery Date'] = delivery_date
        result_df['RFID Start'] = rfid_series[0][0] if rfid_series else ''
        result_df['RFID End'] = rfid_series[0][1] if rfid_series else ''
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        result_df.to_excel(temp_file.name, index=False)
        outputs[f"Processed_{i+1}.xlsx"] = temp_file.name
    return outputs

# --- UI ---
st.title("📦 PO Processor with File Uploads (Paths Mode)")

# Upload Excel files
st.header("1. Upload Excel Files")
uploaded_files = st.file_uploader("Upload Excel files", type="xlsx", accept_multiple_files=True)

# Parameters
st.header("2. Enter Parameters")
delivery_date = st.date_input("Delivery Date")
rfid_series_str = st.text_input("RFID Series (e.g., C52767864-C52768000,C56916036-C56917000)")

# Run
if st.button("Run Parser"):
    if not uploaded_files:
        st.error("Please upload at least one Excel file.")
    elif not rfid_series_str:
        st.error("Please enter RFID series.")
    else:
        rfid_series = [x.strip().split('-') for x in rfid_series_str.split(',')]

        # Save uploaded files to temp files and get paths
        temp_paths = []
        for file in uploaded_files:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            temp_file.write(file.read())
            temp_file.close()
            temp_paths.append(temp_file.name)

        # Simulate two fixed config files (write from DataFrame or read from disk)
        config_path1 = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx').name
        config_path2 = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx').name
        pd.DataFrame({'Setting': ['A'], 'Value': [1]}).to_excel(config_path1, index=False)
        pd.DataFrame({'Option': ['X'], 'Enabled': [True]}).to_excel(config_path2, index=False)

        # Run parser
        st.info("Running parser...")
        output_files = run_po_parser(
            delivery_date.strftime("%m/%d/%Y"), rfid_series,
            temp_paths, config_path1, config_path2
        )
        st.success("Done!")

        st.header("3. Download Files")
        for filename, filepath in output_files.items():
            with open(filepath, 'rb') as f:
                st.download_button(label=f"Download {filename}", data=f, file_name=filename)

        # Track files for cleanup
        st.session_state['temp_files'] = temp_paths + list(output_files.values()) + [config_path1, config_path2]

# Cleanup temp files on reload
if 'temp_files' in st.session_state:
    for fpath in st.session_state['temp_files']:
        try:
            os.remove(fpath)
        except Exception:
            pass
    del st.session_state['temp_files']
