import pandas as pd;
import re;

data = pd.read_csv('Data.csv'); 
def replace_char(dataframe, replacement): 
    for before_char, after_char in replacement.items():
        dataframe = dataframe.replace(before_char, after_char, regex=True); 
    return dataframe; 

def reorganize(data): 
    replacement = {
        r'[^a-zA-Z0-9,]+': ' ',
        r'^\s+': '',
        r'\s+': ' ',
    }
    data = replace_char(data, replacement); 
    pattern = r'(\d+)[(),]*([A-Z]{2})-([A-Z])(\d{4})'; 
    def rearrange(match): 
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}-{match.group(4)}"; 
    data = data.map(lambda x: re.sub(pattern, rearrange, x) if isinstance(x, str) else x); 
    return data; 

data = reorganize(data); 
count_lines = data.shape[0]; 
array_maxSize = count_lines//4; 

dataframes = []; 
for i in range(4):
    start = i * array_maxSize; 
    end = (i + 1) * array_maxSize; 
    part = data.iloc[start:end]; 
    field = part.values.tolist(); 
    df_field = pd.DataFrame(field, columns=[f'field_{i + 1}']); 
    dataframes.append(df_field); 

new_dataframe = pd.concat(dataframes, axis=1); 
new_dataframe.to_excel('Data_filtered.xlsx', index=False); 
print("Dados salvos em 'Data_filtered.xlsx'"); 