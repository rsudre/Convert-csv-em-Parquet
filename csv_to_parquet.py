import pandas as pd

def csv_to_parquet(input_csv, output_parquet):
    """
    Converte um arquivo CSV em um arquivo Parquet.
    """
    try:
        # Carregar o CSV para um DataFrame
        df = pd.read_csv(input_csv)
        # Salvar o DataFrame como arquivo Parquet
        df.to_parquet(output_parquet, index=False)
        print(f"Arquivo convertido com sucesso: {output_parquet}")
    except Exception as e:
        print(f"Erro ao converter CSV para Parquet: {e}")

def read_and_display_parquet(parquet_file):
    """
    Lê e exibe o conteúdo de um arquivo Parquet.
    """
    try:
        # Carregar o arquivo Parquet para um DataFrame
        df = pd.read_parquet(parquet_file)
        # Exibir as primeiras linhas do DataFrame
        print("Conteúdo do arquivo Parquet:")
        print(df.head())  # Exibir apenas as primeiras 5 linhas
    except Exception as e:
        print(f"Erro ao ler o arquivo Parquet: {e}")

# Exemplo de uso
if __name__ == "__main__":
    csv_input_file = '/tmp/test.csv'
    parquet_output_file = '/tmp/test.parquet'

    # Ativar/desativar conversão ou leitura
    run_conversion = True
    run_read = True

    if run_conversion:
        csv_to_parquet(csv_input_file, parquet_output_file)
    
    if run_read:
        read_and_display_parquet(parquet_output_file)
