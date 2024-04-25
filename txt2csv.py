# file exports are in an format that doesnt play nice with importing to db
# converts to csv, removes bad columns 
import os 
import pandas as pd 

# TODO: clean this up, add func to accept just 1 file

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

def prep_files(path_in, path_out):
  files = os.listdir(path_in)
  for i in files:
    output = os.path.join(path_out, f"processed_{i}")
    try:
      df = pd.read_csv(os.path.join(path_in, i), delimiter="\t")
      df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
      df.to_csv(output, index=False)
      print('Success: %s -> %s' % (i, output)
    except Exception as e:
      print('%s Failed With Error: %s' % (i, e)
      print('\nSkipping to next file.)
      pass

if __name__ == '__main__':
    prep_files(path_in='thepathtolookin',path_out='wheretosave')
