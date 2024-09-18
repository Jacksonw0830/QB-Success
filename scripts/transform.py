import pandas as pd
from extract import read_csv_files  # Import your existing extract function

def compare_transitions(ncaa_df, nfl_df):
    """Compares players who transitioned from NCAA to NFL."""
    common_players = pd.merge(ncaa_df, nfl_df, on='player', suffixes=('_ncaa', '_nfl'))
    
    # Calculate differences in key performance metrics
    common_players['ypa_change'] = common_players['ypa_nfl'] - common_players['ypa_ncaa']
    common_players['touchdowns_change'] = (common_players['touchdowns_nfl'] / common_players['player_game_count_nfl']) - (common_players['touchdowns_ncaa'] / common_players['player_game_count_ncaa'])
    common_players['accuracy_percent_change'] = common_players['accuracy_percent_nfl'] - common_players['accuracy_percent_ncaa']
    
    return common_players[['player', 'ypa_change', 'touchdowns_change', 'accuracy_percent_change']]

if __name__ == "__main__":
    # Use the function from extract.py to read all the CSV files
    nfl_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/nfl', 'NFL')
    ncaa_df = read_csv_files('/home/jx2jetpl4ne/Documents/etl_project/ncaa', 'NCAA')

    # Compare player transitions
    transitions = compare_transitions(ncaa_df, nfl_df)
    
    # Display the comparison results
    print(transitions.head())
