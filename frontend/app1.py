import datetime
import os
import sys

import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from backend.getfilters import DataFilter
from backend.querybuilder import QueryBuilder, get_db_connection

# Initialize session state variables
if "csv_data" not in st.session_state:
    st.session_state.csv_data = None
if "query_executed" not in st.session_state:
    st.session_state.query_executed = ""
if "all_query" not in st.session_state:
    st.session_state.all_query = ""
if "params" not in st.session_state:
    st.session_state.params = []
if "download_prepared" not in st.session_state:
    st.session_state.download_prepared = False

config_file = "config.toml"
data_filter = DataFilter(config_file)
filters = data_filter.getfilters()
filters_types = data_filter.getfiltersTypes()
db_config = data_filter.getDB()

st.set_page_config(
    page_title="Jaktim Data Explorer",
    layout="wide",
    page_icon="./assets/djp.ico",
    initial_sidebar_state="expanded",
)

# Your existing CSS styles here
st.markdown(
    """
    <style type="text/css">
    #root {
        margin: 1rem;
        padding: 1rem;
        font-size: 20px;
        border-left: 5px solid rgb(230, 234, 241);
        background-color: rgb(33,44,95);
    }
    [data-testid="stHeader"] {
        display: none;
    }
    [data-testid="stWidgetLabel"]{
        color: #FFFFFF;
    }

    [data-testid="stSidebar"] {
        background-color: rgb(33,44,95);
        color: #FFFFFF;
    }
    [input[aria-label="Download All Rows"]{
        color:rgb(33,44,95);
    }

    [data-testid="stMain"]{
        background-color: #FFFFFF;
        color: rgb(33,44,95);
    }

    blockquote p {
        font-size: 30px;
        color: #FFFFFF;
    }

    [aria-selected="true"] {
         color: #000000;
    }
    </style>
""",
    unsafe_allow_html=True,
)


@st.cache_data
def downloadCSV(conn, all_query, params):
    return conn.execute(all_query, params).fetchdf().to_csv(index=False)


def getDataFilter():
    column_names = []
    column_types = []
    filter_values = []

    # Only process keys that are actual filters
    for key in filters_types.keys():
        if (
            key in st.session_state
            and isinstance(st.session_state[key], (list, int, tuple))
            and len(str(st.session_state[key])) > 0
        ):
            column_names.append(key)
            column_types.append(filters_types[key])
            if filters_types[key] == "datetime":
                dates = [x.isoformat() for x in st.session_state[key]]
                filter_values.append(dates)
            else:
                filter_values.append(st.session_state[key])

    filters_operator = [
        "IN" if coltype == "string" else "" if coltype == "datetime" else "="
        for coltype in column_types
    ]
    return column_names, column_types, filters_operator, filter_values


def prepare_download():
    st.session_state.download_prepared = True


# Sidebar filters
with st.sidebar:
    st.title("Data Explorer:🎈")
    now = datetime.datetime.now()
    for key, type in filters_types.items():
        if type == "string":
            st.session_state[key] = st.multiselect(
                label=key,
                placeholder="Choose one or more",
                options=filters[key],
                default=st.session_state.get(key, []),
            )
        elif type == "integer":
            st.session_state[key] = st.number_input(
                label=key,
                min_value=0,
                max_value=1000000000,
                step=1,
                value=st.session_state.get(key, 0),
            )
        elif type == "datetime":
            begin_date = datetime.datetime(filters[key][0], 1, 1)
            end_date = datetime.datetime(filters[key][1], 12, 31)
            st.session_state[key] = st.date_input(
                key,
                value=st.session_state.get(
                    key, (datetime.datetime(filters[key][1], 1, 1), end_date)
                ),
                min_value=begin_date,
                max_value=end_date,
                format="YYYY-MM-DD",
            )
    querydata = st.button(label="Apply", type="primary", use_container_width=True)

# Main content
if querydata:
    with st.spinner("Loading data..."):
        column_names, column_types, filters_operator, filters_value = getDataFilter()

        builder = QueryBuilder(f"db.{db_config['schema']}.{db_config['database']}")
        builder.add_condition(
            column_names=column_names,
            column_types=column_types,
            operators=filters_operator,
            values=filters_value,
        )
        query, params = builder.build_select()

        conn = get_db_connection()
        result = conn.execute(query, params).fetchdf()
        all_query = query.replace("LIMIT 200", "").strip()

        st.title("Sampling Data")
        st.dataframe(result, use_container_width=True, hide_index=True)
        st.success("Data loaded successfully!")

        st.session_state.all_query = all_query
        st.session_state.params = params
        st.session_state.query_executed = "Yes"
        st.session_state.download_prepared = False

if st.session_state.query_executed == "Yes":
    if not st.session_state.download_prepared:
        st.button(
            "Prepare data for download", on_click=prepare_download, type="primary"
        )

    if st.session_state.download_prepared:
        if st.session_state.csv_data is None:
            with st.spinner("Preparing download..."):
                conn = get_db_connection()
                st.session_state.csv_data = downloadCSV(
                    conn, st.session_state.all_query, st.session_state.params
                )

        st.download_button(+6
            label="Download CSV",
            file_name=f"all_rows_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            data=st.session_state.csv_data,
            mime="text/csv",
            type="primary",
            help="Download may take a few seconds to prepare",
        )

    with st.expander("Query"):
        st.write(st.session_state.all_query)
