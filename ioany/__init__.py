__version__ = "0.0.6"
from .core import (
    DataFrame,
    DataFrameStatic,
    DataFrameIterative,
    read_fixed,
    read_csv, load_csv, slurp_csv, save_csv,
    load_json, slurp_json, save_json,
    read_lines, slurp_lines, save_lines,
    read_recs, slurp_recs, save_recs,
    slurp_text, save_text,
    save_content,
)

