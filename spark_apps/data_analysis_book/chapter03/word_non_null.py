from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

conf = SparkConf()\
        .set('spark.yarn.maxAppAttempts', '1')\
        .set('spark.hadoop.fs.s3a.endpoint', os.getenv('HOST_MINIO', None))\
        .set('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACESS_KEY', None))\
        .set('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_KEY', None))\
        .set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')\
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .set("spark.sql.extensions", "com.databricks.spark.xml")

spark = SparkSession.builder.config(conf=conf).appName('MyApp').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
        
# builder = pyspark.sql.SparkSession.builder.appName('MyApp') \
#         .config('spark.yarn.maxAppAttempts', '1') \
#         .config('spark.hadoop.fs.s3a.endpoint', 'http://192.168.59.128:9000') \
#         .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACESS_KEY', None)) \
#         .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_KEY', None)) \
#         .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
#         .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
# spark = builder.getOrCreate()


# Função que retorna o nome que será colocado no arquivo
def filename(dataframe):

    dataframe = dataframe.select('SourceFileName')

    dataframe = dataframe.withColumn('data_ExecutionAPI', split(dataframe['SourceFileName'], '_').getItem(2)) \
                        .withColumn('hora_ExecutionAPI', split(dataframe['SourceFileName'], '_').getItem(3))

    dataframe = dataframe.select('data_ExecutionAPI','hora_ExecutionAPI').distinct()

    dataframe = dataframe.withColumn('hora_ExecutionAPI', regexp_replace('hora_ExecutionAPI', '.xml', ''))

    filename = 'listaCargas-'+ dataframe.select('data_ExecutionAPI').first()[0] + '-' + dataframe.select('hora_ExecutionAPI').first()[0] + '.parquet' 

    return filename

# Função para salvar dados na camada bronze
def write_bronze(dataframe, bucket, path, data_format, mode, SourceName):
    print("\nEscrevendo os dados lidos da transient zone para parquet na bronze zone...")
    try:
        # Adicione uma coluna contendo o número de linhas no DataFrame
        num_rows = dataframe.count()
        dataframe = dataframe.withColumn('NumRows', lit(num_rows).cast(StringType()))

        # Adicione uma coluna contendo a data de processamento
        dataframe = dataframe.withColumn('ProcessedDate', date_format(current_date(), 'dd-MM-yyyy'))

        # Adicione uma coluna contendo a data e hora de processamento
        dataframe = dataframe.withColumn('ProcessedTime', current_timestamp())

        # Adicione uma coluna contendo o nome da fonte de dados
        dataframe = dataframe.withColumn('SourceName', lit(SourceName))

        dataframe.coalesce(1)\
            .write\
            .format(data_format)\
            .mode(mode)\
            .option("compression", "snappy")\
            .save(f'{bucket}/{path}')
        print(f'Dados escritos na bronze zone com sucesso!')
        return 0
    except Exception as err:
        print(f'Falha para escrever dados na bronze: {err}')
        return 1

# Função para expandir a coluna com Json para linhas 
def explode_cols( df, col_explode):
    df = df.withColumn('exp_colunas', arrays_zip(*col_explode))
    df = df.withColumn('exp_colunas', explode('exp_colunas'))
    for colm in col_explode:
        final_col = 'exp_colunas.'+ colm 
        df = df.withColumn(final_col, col(final_col))
        
    return df.drop(
                    col('exp_colunas'),
                    col('t10_id'),
                    col('t10_data_grava'),
                    col('t10_sn_planejada'),
                    col('t10_destino'),
                    col('t10_data_saida'),
                    col('t10_data_prevista_termino'),
                    col('t06_id'),
                    col('t06_descricao'),
                    col('t06_placa'),
                    col('t06_peso_max_entregas'),
                    col('t06_volume_max_entregas'),
                    col('t06_qtdMaxEntregas'),
                    col('t06_modelo'),
                    col('t06_dataHoraAtual'),
                    col('t06_latitudeAtual'),
                    col('t06_longitudeAtual'),
                    col('t05_id'),
                    col('t05_codigo_erp'),
                    col('t05_nome'),
                    col('t43_fantasia'),
                    col('t10_motorista_id'),
                    col('t10_ajudante1_id'),
                    col('t05_codigo_erp_ajudante1'),
                    col('t05_nome_ajudante1'),
                    col('t10_ajudante2_id'),
                    col('t05_codigo_erp_ajudante2'),
                    col('t05_nome_ajudante2'),
                    col('t10_ajudante3_id'),
                    col('t05_codigo_erp_ajudante3'),
                    col('t05_nome_ajudante3'),
                    col('t10_ajudante4_id'),
                    col('t05_codigo_erp_ajudante4'),
                    col('t05_nome_ajudante4'),
                    col('t10_transporte_id'),
                    col('t10_situacao'),
                    col('t10_carga_formada_erp'),
                    col('t10_sn_deve_formar_carga_erp'),
                    col('t10_status_integracao'),
                    col('t10_retorno_internalizacao'),
                    col('t10_senha_mobile_cast'),
                    col('t10_custo_estimado'),
                    col('t10_diarias_completas'),
                    col('t10_diarias_simples'),
                    col('t10_kmTotal'),
                    col('t10_qtd_almoco'),
                    col('t10_qtd_cafe_manha'),
                    col('t10_qtd_hospedagens'),
                    col('t10_qtd_jantar'),
                    col('t10_qtd_pallets_carga'),
                    col('t10_sn_frete_negociado'),
                    col('t10_sn_utiliza_chapa'),
                    col('t10_tempoTotal'),
                    col('t10_tipo_chapa'),
                    col('t10_valor_adiantado_motorista'),
                    col('t10_valor_adiantamento_extra'),
                    col('t10_valor_chapa'),
                    col('t10_valor_descarrego'),
                    col('t10_valor_frete_negociado'),
                    col('peso_entregas'),
                    col('qtd_pallets_entregas'),
                    col('valor_entregas'),
                    col('volume_entregas'),
                    col('qtd_clientes'),
                    col('clientes'),
                    col('qtd_entregas'),
                    col('t141_id'),
                    col('t141_descricao'),
                    col('data_atualizacao'),
                    col('responsavel_atualizacao'))

# Função que retorna um objeto para conectar ao MinIO
def accessMinio():
    s3 = boto3.client('s3',
                    endpoint_url=os.getenv('HOST_MINIO', None),
                    aws_access_key_id=os.getenv("AWS_ACESS_KEY", None),
                    aws_secret_access_key=os.getenv("AWS_SECRET_KEY", None),
                    config=Config(signature_version='s3v4')
                    )
    try:
        s3.list_buckets()
        return s3
    except Exception as e:
        print(f"Erro ao acessar MinIO: {e}")
        raise SystemExit

# Função para excluir todos os objetos de uma pasta do bucket
def delete_all_objects_bucket(s3, bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    files_in_folder = response["Contents"]
    files_to_delete = []
    
    for f in files_in_folder:
        files_to_delete.append({"Key": f["Key"]})

    try:
        response = s3.delete_objects(
            Bucket=bucket, Delete={"Objects": files_to_delete}
        )
        print(f"Objetos deletados do bucket: {bucket}/{prefix}")
    except ClientError:
        print("Erro ao deletar objetos do bucket")
        raise

# Estrutura do json do endpoint 'listaCargas'
schema_listaCargas = ArrayType(
            StructType([
                StructField('t10_id', StringType(), True),
                StructField('t10_data_grava', StringType(), True),
                StructField('t10_sn_planejada', StringType(), True),
                StructField('t10_destino', StringType(), True),
                StructField('t10_data_saida', StringType(), True),
                StructField('t10_data_prevista_termino', StringType(), True),
                StructField('t06_id', StringType(), True),
                StructField('t06_descricao', StringType(), True),
                StructField('t06_placa', StringType(), True),
                StructField('t06_peso_max_entregas', StringType(), True),
                StructField('t06_volume_max_entregas', StringType(), True),
                StructField('t06_qtdMaxEntregas', StringType(), True),
                StructField('t06_modelo', StringType(), True),
                StructField('t06_dataHoraAtual', StringType(), True),
                StructField('t06_latitudeAtual', StringType(), True),
                StructField('t06_longitudeAtual', StringType(), True),
                StructField('t05_id', StringType(), True),
                StructField('t05_codigo_erp', StringType(), True),
                StructField('t05_nome', StringType(), True),
                StructField('t43_fantasia', StringType(), True),
                StructField('t10_motorista_id', StringType(), True),
                StructField('t10_ajudante1_id', StringType(), True),
                StructField('t05_codigo_erp_ajudante1', StringType(), True),
                StructField('t05_nome_ajudante1', StringType(), True),
                StructField('t10_ajudante2_id', StringType(), True),
                StructField('t05_codigo_erp_ajudante2', StringType(), True),
                StructField('t05_nome_ajudante2', StringType(), True),
                StructField('t10_ajudante3_id', StringType(), True),
                StructField('t05_codigo_erp_ajudante3', StringType(), True),
                StructField('t05_nome_ajudante3', StringType(), True),
                StructField('t10_ajudante4_id', StringType(), True),
                StructField('t05_codigo_erp_ajudante4', StringType(), True),
                StructField('t05_nome_ajudante4', StringType(), True),
                StructField('t10_transporte_id', StringType(), True),
                StructField('t10_situacao', StringType(), True),
                StructField('t10_carga_formada_erp', StringType(), True),
                StructField('t10_sn_deve_formar_carga_erp', StringType(), True),
                StructField('t10_status_integracao', StringType(), True),
                StructField('t10_retorno_internalizacao', StringType(), True),
                StructField('t10_senha_mobile_cast', StringType(), True),
                StructField('t10_custo_estimado', StringType(), True),
                StructField('t10_diarias_completas', StringType(), True),
                StructField('t10_diarias_simples', StringType(), True),
                StructField('t10_kmTotal', StringType(), True),
                StructField('t10_qtd_almoco', StringType(), True),
                StructField('t10_qtd_cafe_manha', StringType(), True),
                StructField('t10_qtd_hospedagens', StringType(), True),
                StructField('t10_qtd_jantar', StringType(), True),
                StructField('t10_qtd_pallets_carga', StringType(), True),
                StructField('t10_sn_frete_negociado', StringType(), True),
                StructField('t10_sn_utiliza_chapa', StringType(), True),
                StructField('t10_tempoTotal', StringType(), True),
                StructField('t10_tipo_chapa', StringType(), True),
                StructField('t10_valor_adiantado_motorista', StringType(), True),
                StructField('t10_valor_adiantamento_extra', StringType(), True),
                StructField('t10_valor_chapa', StringType(), True),
                StructField('t10_valor_descarrego', StringType(), True),
                StructField('t10_valor_frete_negociado', StringType(), True),
                StructField('peso_entregas', StringType(), True),
                StructField('qtd_pallets_entregas', StringType(), True),
                StructField('valor_entregas', StringType(), True),
                StructField('volume_entregas', StringType(), True),
                StructField('qtd_clientes', StringType(), True),
                StructField('clientes', StringType(), True),
                StructField('qtd_entregas', StringType(), True),
                StructField('t141_id', StringType(), True),
                StructField('t141_descricao', StringType(), True),
                StructField('data_atualizacao', StringType(), True),
                StructField('responsavel_atualizacao', StringType(), True)]))

# Leitura dos xml para criação do df
df_xml = spark.read.format('xml').options(rowTag='ns1:listaCargasResponse', excludeAttribute='true').load('s3a://laredo-datalake-transient-zone/fusion/listaCargas/data/')

# Adiciona uma coluna com o nome do arquivo de leitura
df_xml = df_xml.withColumn('SourceFileName', input_file_name())

# Adiciona uma coluna no df contendo somente os elementos do Json
df_parsed = df_xml.withColumn('json_parsed', from_json(df_xml['resParam'], schema_listaCargas)).drop('resParam')

# Etapa para juntar em colunas cada informação do endpoint-> t10_id:[1, 2, 3] | t10_destino: [a, b, c]
df_json = df_parsed.select(
                'SourceFileName',
                'json_parsed.t10_id',
                'json_parsed.t10_data_grava',
                'json_parsed.t10_sn_planejada',
                'json_parsed.t10_destino',
                'json_parsed.t10_data_saida',
                'json_parsed.t10_data_prevista_termino',
                'json_parsed.t06_id',
                'json_parsed.t06_descricao',
                'json_parsed.t06_placa',
                'json_parsed.t06_peso_max_entregas',
                'json_parsed.t06_volume_max_entregas',
                'json_parsed.t06_qtdMaxEntregas',
                'json_parsed.t06_modelo',
                'json_parsed.t06_dataHoraAtual',
                'json_parsed.t06_latitudeAtual',
                'json_parsed.t06_longitudeAtual',
                'json_parsed.t05_id',
                'json_parsed.t05_codigo_erp',
                'json_parsed.t05_nome',
                'json_parsed.t43_fantasia',
                'json_parsed.t10_motorista_id',
                'json_parsed.t10_ajudante1_id',
                'json_parsed.t05_codigo_erp_ajudante1',
                'json_parsed.t05_nome_ajudante1',
                'json_parsed.t10_ajudante2_id',
                'json_parsed.t05_codigo_erp_ajudante2',
                'json_parsed.t05_nome_ajudante2',
                'json_parsed.t10_ajudante3_id',
                'json_parsed.t05_codigo_erp_ajudante3',
                'json_parsed.t05_nome_ajudante3',
                'json_parsed.t10_ajudante4_id',
                'json_parsed.t05_codigo_erp_ajudante4',
                'json_parsed.t05_nome_ajudante4',
                'json_parsed.t10_transporte_id',
                'json_parsed.t10_situacao',
                'json_parsed.t10_carga_formada_erp',
                'json_parsed.t10_sn_deve_formar_carga_erp',
                'json_parsed.t10_status_integracao',
                'json_parsed.t10_retorno_internalizacao',
                'json_parsed.t10_senha_mobile_cast',
                'json_parsed.t10_custo_estimado',
                'json_parsed.t10_diarias_completas',
                'json_parsed.t10_diarias_simples',
                'json_parsed.t10_kmTotal',
                'json_parsed.t10_qtd_almoco',
                'json_parsed.t10_qtd_cafe_manha',
                'json_parsed.t10_qtd_hospedagens',
                'json_parsed.t10_qtd_jantar',
                'json_parsed.t10_qtd_pallets_carga',
                'json_parsed.t10_sn_frete_negociado',
                'json_parsed.t10_sn_utiliza_chapa',
                'json_parsed.t10_tempoTotal',
                'json_parsed.t10_tipo_chapa',
                'json_parsed.t10_valor_adiantado_motorista',
                'json_parsed.t10_valor_adiantamento_extra',
                'json_parsed.t10_valor_chapa',
                'json_parsed.t10_valor_descarrego',
                'json_parsed.t10_valor_frete_negociado',
                'json_parsed.peso_entregas',
                'json_parsed.qtd_pallets_entregas',
                'json_parsed.valor_entregas',
                'json_parsed.volume_entregas',
                'json_parsed.qtd_clientes',
                'json_parsed.clientes',
                'json_parsed.qtd_entregas',
                'json_parsed.t141_id',
                'json_parsed.t141_descricao',
                'json_parsed.data_atualizacao',
                'json_parsed.responsavel_atualizacao')

# Variável com todas as colunas do endpoint para repassar na função explode_cols
colunas_listaCargas = ['t10_id',
                        't10_data_grava',
                        't10_sn_planejada',
                        't10_destino',
                        't10_data_saida',
                        't10_data_prevista_termino',
                        't06_id',
                        't06_descricao',
                        't06_placa',
                        't06_peso_max_entregas',
                        't06_volume_max_entregas',
                        't06_qtdMaxEntregas',
                        't06_modelo',
                        't06_dataHoraAtual',
                        't06_latitudeAtual',
                        't06_longitudeAtual',
                        't05_id',
                        't05_codigo_erp',
                        't05_nome',
                        't43_fantasia',
                        't10_motorista_id',
                        't10_ajudante1_id',
                        't05_codigo_erp_ajudante1',
                        't05_nome_ajudante1',
                        't10_ajudante2_id',
                        't05_codigo_erp_ajudante2',
                        't05_nome_ajudante2',
                        't10_ajudante3_id',
                        't05_codigo_erp_ajudante3',
                        't05_nome_ajudante3',
                        't10_ajudante4_id',
                        't05_codigo_erp_ajudante4',
                        't05_nome_ajudante4',
                        't10_transporte_id',
                        't10_situacao',
                        't10_carga_formada_erp',
                        't10_sn_deve_formar_carga_erp',
                        't10_status_integracao',
                        't10_retorno_internalizacao',
                        't10_senha_mobile_cast',
                        't10_custo_estimado',
                        't10_diarias_completas',
                        't10_diarias_simples',
                        't10_kmTotal',
                        't10_qtd_almoco',
                        't10_qtd_cafe_manha',
                        't10_qtd_hospedagens',
                        't10_qtd_jantar',
                        't10_qtd_pallets_carga',
                        't10_sn_frete_negociado',
                        't10_sn_utiliza_chapa',
                        't10_tempoTotal',
                        't10_tipo_chapa',
                        't10_valor_adiantado_motorista',
                        't10_valor_adiantamento_extra',
                        't10_valor_chapa',
                        't10_valor_descarrego',
                        't10_valor_frete_negociado',
                        'peso_entregas',
                        'qtd_pallets_entregas',
                        'valor_entregas',
                        'volume_entregas',
                        'qtd_clientes',
                        'clientes',
                        'qtd_entregas',
                        't141_id',
                        't141_descricao',
                        'data_atualizacao',
                        'responsavel_atualizacao']

# Chamada da função para expandir as colunas para linhas
df_explode = explode_cols(df_json, colunas_listaCargas)

# renomear colunas do df
colunas_renomeadas = {coluna: coluna.split('.')[1] for coluna in df_explode.columns if '.' in coluna}

# Loop para renomear as colunas do df retirando o prefixo (exp_colunas.)
for nome_antigo, nome_novo in colunas_renomeadas.items():
    df_explode = df_explode.withColumnRenamed(nome_antigo, nome_novo)

# Cria um df somente com algumas colunas para exemplo
df_parquet = df_explode.select('t10_id', 't10_data_grava', 't10_destino', 't06_placa', 't10_situacao', 't06_id', 'SourceFileName')

# df_parquet = df_parquet.limit(2)

df_parquet.show(truncate=False)

# Variáveis para a função write_bronze
path = filename(df_xml)
bucket_bronze = 's3a://laredo-datalake-bronze-zone/fusion/listaCargas'
write_bronze(df_parquet, bucket_bronze, path, 'parquet', 'overwrite', 'fusion')

# Deletar xml do bucket transient-zone
bucket_name_delete = 'laredo-datalake-transient-zone'
bucket_prefix_delete = 'fusion/listaCargas/data/'
s3 = accessMinio()
delete_all_objects_bucket(s3,bucket_name_delete, bucket_prefix_delete)

# Encerrando a sessão Spark
spark.stop()