import pandas as pd;
import re;

data = pd.read_csv('Data_Organizer/Data.csv'); 
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
print("Escolha o tipo de arquivo em que os dados tratados serão salvos: "); 
print("1. Excel (.xlsx)"); 
print("2. CSV (.csv)"); 
print("3. Parquet (.parquet)"); 
print("4. HDF5 (.H5)"); 
print("5. JSON (.json)"); 
output_directory = input("Digite o caminho do diretório de destino: ")
option = input("Selecione o número correspondente ao tipo de arquivo: "); 
match option:
    case "1":
        output_path = f"{output_directory}/Data_filtered.xlsx"
        new_dataframe.to_excel(output_path, index=False);      
        print("Dados salvos em 'Data_filtered.xlsx'"); 
    case "2":
        output_path = f"{output_directory}/Data_filtered.csv"
        new_dataframe.to_csv(output_path, index=False);      
        print("Dados salvos em 'Data_filtered.csv'"); 
    case "3":
        output_path = f"{output_directory}/Data_filtered.parquet"
        new_dataframe.to_parquet(output_path, index=False);      
        print("Dados salvos em 'Data_filtered.parquet'"); 
    case "4":
        output_path = f"{output_directory}/Data_filtered.h5"
        new_dataframe.to_hdf(output_path, index=False);      
        print("Dados salvos em 'Data_filtered.h5'"); 
    case "5":
        output_path = f"{output_directory}/Data_filtered.json"
        new_dataframe.to_json(output_path, index=False);      
        print("Dados salvos em 'Data_filtered.json'"); 
    case _:
        print("Por favor, selecione uma opção válida!"); 
        option = input("Selecione o número correspondente ao tipo de arquivo: "); 