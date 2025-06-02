import tempfile
from pathlib import Path

import streamlit as st


def export_duckdb_data(conn, query, file_format="xlsx"):
    """Export data using DuckDB's native COPY command"""
    # Create a temporary directory that will be automatically cleaned up
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create full file path
        temp_file = Path(temp_dir) / f"exported_data.{file_format}"

        # Construct and execute COPY command based on format
        if file_format == "xlsx":
            copy_query = f"""COPY ({query}) TO '{temp_file}' 
                           WITH (FORMAT EXCEL)"""
            mime_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        elif file_format == "csv":
            copy_query = f"""COPY ({query}) TO '{temp_file}'
                           WITH (HEADER TRUE, DELIMITER ',')"""
            mime_type = "text/csv"

        # Execute the COPY command
        conn.execute(copy_query)

        # Read the file into memory
        with open(temp_file, "rb") as f:
            data = f.read()

        return data, mime_type


def create_duckdb_download_button(
    conn, query, button_text="Download Data", file_format="xlsx"
):
    """Create a Streamlit download button for DuckDB export"""
    try:
        data, mime_type = export_duckdb_data(conn, query, file_format)

        st.download_button(
            label=button_text,
            data=data,
            file_name=f"exported_data.{file_format}",
            mime=mime_type,
        )
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")


# Usage example
def show_export_options(conn, query):
    # Add progress indicator for large datasets
    if st.button("Prepare Export"):
        with st.spinner("Preparing your data export..."):
            # col1, col2 = st.columns(2)
            # with col1:
            # create_duckdb_download_button(conn, query, "Download Excel", "xlsx")
            # with col2:
            create_duckdb_download_button(conn, query, "Download CSV", "csv")
