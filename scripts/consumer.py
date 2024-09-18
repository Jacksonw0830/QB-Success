import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

# Load NCAA data from 2018 to 2022
ncaa_files = glob.glob('/home/jx2jetpl4ne/Documents/etl_project/ncaa/passing_summary(201[8-9]).csv') + \
             glob.glob('/home/jx2jetpl4ne/Documents/etl_project/ncaa/passing_summary(202[0-2]).csv')

if ncaa_files:
    ncaa_data = pd.concat((pd.read_csv(f) for f in ncaa_files), ignore_index=True)
else:
    print("No NCAA files found to load.")

# Load NFL 2023 data
nfl_files = glob.glob('/home/jx2jetpl4ne/Documents/etl_project/nfl/passing_summary(2023).csv')
if nfl_files:
    nfl_data = pd.concat((pd.read_csv(f) for f in nfl_files), ignore_index=True)

# Normalize player names (strip spaces and convert to lowercase)
nfl_data['player'] = nfl_data['player'].str.lower().str.strip()
ncaa_data['player'] = ncaa_data['player'].str.lower().str.strip()

# Filter for QBs only in both datasets
nfl_data = nfl_data[nfl_data['position'] == 'QB']
ncaa_data = ncaa_data[ncaa_data['position'] == 'QB']

# Combine the college data into one aggregate variable for easy viewing
college_aggregate = ncaa_data.groupby('player').agg({
    'grades_pass': 'mean',  # Average passing grade over the years
    'turnover_worthy_plays': 'mean',  # Average turnover-worthy plays
    'attempts': 'sum',  # Total pass attempts (or any other useful metrics)
    'yards': 'sum'
}).reset_index()

# Match NFL players with their college data
nfl_data_filtered = nfl_data[nfl_data['player'].isin(college_aggregate['player'])]

# Merge NFL and NCAA (college) data on player name for plotting
merged_data = pd.merge(nfl_data_filtered, college_aggregate, on='player', suffixes=('_nfl', '_ncaa'))

# Set up the plot
plt.figure(figsize=(14, 6))

# NFL Plot (Turnover Worthy Plays vs. Passing Grades in NFL 2023)
plt.subplot(1, 2, 1)  # 1 row, 2 columns, first subplot
sns.scatterplot(x='turnover_worthy_plays_nfl', y='grades_pass_nfl', data=merged_data)
plt.title('NFL 2023: Turnover Worthy Plays vs. Passing Grades for QBs')
plt.xlabel('Turnover Worthy Plays (NFL)')
plt.ylabel('Passing Grade (NFL)')

# Label each point with the player's name using annotation to avoid overlap
for i in range(merged_data.shape[0]):
    plt.annotate(
        merged_data['player'].iloc[i].title(),
        (merged_data['turnover_worthy_plays_nfl'].iloc[i], merged_data['grades_pass_nfl'].iloc[i]),
        textcoords="offset points",  # Position the label slightly away
        xytext=(5, 5),  # Adjust label offset
        ha='right',  # Horizontal alignment
        fontsize=8,
        alpha=0.8
    )

plt.grid(True)

# NCAA Plot (College 2018-2022 Turnover Worthy Plays vs. Passing Grades)
plt.subplot(1, 2, 2)  # 1 row, 2 columns, second subplot
sns.scatterplot(x='turnover_worthy_plays_ncaa', y='grades_pass_ncaa', data=merged_data)
plt.title('NCAA 2018-2022: Turnover Worthy Plays vs. Passing Grades for NFL QBs')
plt.xlabel('Turnover Worthy Plays (NCAA)')
plt.ylabel('Passing Grade (NCAA)')

# Label each point with the player's name and "years in college" using annotation
for i in range(merged_data.shape[0]):
    plt.annotate(
        f"{merged_data['player'].iloc[i].title()} (2018-2022)",
        (merged_data['turnover_worthy_plays_ncaa'].iloc[i], merged_data['grades_pass_ncaa'].iloc[i]),
        textcoords="offset points",  # Position the label slightly away
        xytext=(5, 5),  # Adjust label offset
        ha='right',  # Horizontal alignment
        fontsize=8,
        alpha=0.8
    )

plt.grid(True)

# Show the plots
plt.tight_layout()
plt.savefig('/mnt/c/Users/JJ_Je/OneDrive/Documents/GitHub/nfl_vs_ncaa_qbs_2018_2022_cleaned.png')
# plt.show()  # Comment this line out if needed
