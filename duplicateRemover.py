'''
This program merge all the csv files and remove duplicates from the merged file and export it to a new csv file named
merged.csv
'''
import pandas as pd
import glob
import os

# merging all the csv files in the current directory
path = os.getcwd()
all_files = glob.glob(path + "/Files/*.csv")
li = []
print(len(all_files))
for filename in all_files:
    # skipping the file named merged.csv and links.csv
    if filename == 'merged.csv' or filename == 'links.csv':
        continue
    df = pd.read_csv(filename, index_col=None, header=0)
    print("File name: ", filename)
    li.append(df)
frame = pd.concat(li, axis=0, ignore_index=True)

# removing duplicates from the merged file
frame.drop_duplicates(subset=['cuit'], keep='first', inplace=True)

# exporting the merged file to a new csv file
frame.to_csv('merged.csv', index=False)
print('Done')
