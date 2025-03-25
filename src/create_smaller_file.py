import pandas as pd
import zipfile
from pathlib import Path

def compress_csv(input_path: str, output_zip_path: str, csv_name: str, nrows: int = 1_000_000) -> None:
    """Reads the first `nrows` rows from a CSV file and compresses it into a ZIP archive."""
    input_path = Path(input_path)
    output_zip_path = Path(output_zip_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    df = pd.read_csv(input_path, nrows=nrows)
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        with zipf.open(csv_name, 'w') as file:
            df.to_csv(file, index=False, line_terminator="\n")
    print(f"File successfully saved to: {output_zip_path}")


# Change file paths where needed.
input_file = "files/aisdk-2024-08-28/aisdk-2024-08-28.csv"
output_zip = "files/aisdk-2024-08-28/aisdk-2024-08-28_small.zip"
csv_filename = "aisdk-2024-08-28_small.csv"

compress_csv(input_file, output_zip, csv_filename)
