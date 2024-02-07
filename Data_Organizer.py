import pandas as pd;
import re;

file_type = input("Digite o tipo do arquivo a ser lido (csv, excel, parquet, json): ")
data = getattr(pd, f'read_{file_type}')()
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
print("\nEscolha o tipo de arquivo em que os dados tratados serão salvos: "); 
print("1. Excel (.xlsx)"); 
print("2. CSV (.csv)"); 
print("3. Parquet (.parquet)"); 
print("4. JSON (.json)"); 

while True:
    output_directory = input("\nDigite o caminho do diretório de destino: "); 
    option = input("Selecione o número correspondente ao tipo de arquivo: "); 
    match option:
        case "1":
            output_path = f"{output_directory}/Data_filtered.xlsx"
            new_dataframe.to_excel(output_path, index=False);      
            print(f"Dados salvos em '{output_path}'"); 
            break
        case "2":
            output_path = f"{output_directory}/Data_filtered.csv"
            new_dataframe.to_csv(output_path, index=False);      
            print(f"Dados salvos em '{output_path}'"); 
            break
        case "3":
            output_path = f"{output_directory}/Data_filtered.parquet"
            new_dataframe.to_parquet(output_path, index=False);      
            print(f"Dados salvos em '{output_path}'"); 
            break
        case "4":
            output_path = f"{output_directory}/Data_filtered.json"
            new_dataframe.to_json(output_path, index=False);      
            print(f"Dados salvos em '{output_path}'"); 
            break
        case _:
            print("\nPor favor, selecione uma opção válida!\n"); 
            continue 