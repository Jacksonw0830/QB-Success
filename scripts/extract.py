import os
import pandas as pd

def read_csv_files(directory, league):
    """Reads all CSV files for either NFL or NCAA."""
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    data_frames = []

    for file in csv_files:
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path)
        df['league'] = league  # Tagging with league type
        data_frames.append(df)

    # Combine all CSV files into a single DataFrame
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df

if __name__ == "__main__":
    # Use absolute paths to point to the correct directories
    nfl_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/nfl', 'NFL')
    ncaa_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/ncaa', 'NCAA')
    print(nfl_df.head())  # Just to check the output
    print(ncaa_df.head())  # Just to check the output

