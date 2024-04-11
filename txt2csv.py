# file exports are in an format that doesnt play nice with importing to db
# converts to csv, removes bad columns 
import os 
import pandas as pd 

file = 'export.txt'
path = 'C:\\Users\\Me\\rawFiles'
filepath = os.path.join(path, file)
outfile = f'fixed_{file.split(".txt")[0]}.csv
outpath = 'C:\\Users\\Me\\processedFiles'
outfilepath = os.path.join(outpath, outfile)

df = pd.read_csv(filepath, delimiter="\t")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df.to_csv(outfilepath, index=False)

print('done')
