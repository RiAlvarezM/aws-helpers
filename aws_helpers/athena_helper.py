import boto3
import time
import re

def start_athena_query(athena_client, query: str, database: str, output_bucket: str, output_path: str) -> dict:
    """
    Inicia una consulta en AWS Athena.

    :param athena_client: Cliente de boto3 para Athena.
    :param query: Consulta SQL a ejecutar.
    :param database: Base de datos en Athena donde se ejecutará la consulta.
    :param output_bucket: Nombre del bucket de S3 donde se guardarán los resultados.
    :param output_path: Ruta dentro del bucket de S3 para almacenar los resultados.
    :return: Respuesta de la ejecución del query, incluyendo el QueryExecutionId.
    """
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': f's3://{output_bucket}/{output_path}'}
    )
    return response

def wait_for_athena_query(athena_client, query_execution_id: str, max_execution: int = 100) -> str:
    """
    Espera a que una consulta de Athena finalice y devuelve el nombre del archivo en S3.

    :param athena_client: Cliente de boto3 para Athena.
    :param query_execution_id: ID de ejecución de la consulta en Athena.
    :param max_execution: Número máximo de intentos antes de detener la espera.
    :return: Nombre del archivo en S3 con los resultados o `False` si falla.
    """
    state = 'RUNNING'

    while max_execution > 0 and state in ['RUNNING', 'QUEUED']:
        max_execution -= 1
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)

        if 'QueryExecution' in response and 'Status' in response['QueryExecution']:
            state = response['QueryExecution']['Status']['State']

            if state == 'FAILED':
                print(f'⚠️ La consulta falló. Quedan {max_execution} intentos.')
                print(response['QueryExecution']['Status'])
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall(r'.*/(.*)', s3_path)[0]
                return filename

        time.sleep(1)  # Espera 1 segundo antes de la siguiente verificación

    return False

def athena_to_s3(athena_client, query: str, database: str, output_bucket: str, output_path: str, max_execution: int = 100) -> str:
    """
    Ejecuta una consulta en Athena y devuelve el nombre del archivo en S3 donde se almacenan los resultados.

    :param athena_client: Cliente de boto3 para Athena.
    :param query: Consulta SQL a ejecutar.
    :param database: Base de datos en Athena.
    :param output_bucket: Nombre del bucket de S3 donde se guardarán los resultados.
    :param output_path: Ruta dentro del bucket de S3 para almacenar los resultados.
    :param max_execution: Número máximo de intentos antes de detener la espera.
    :return: Nombre del archivo en S3 con los resultados o `False` si falla.
    """
    execution = start_athena_query(athena_client, query, database, output_bucket, output_path)
    execution_id = execution['QueryExecutionId']
    return wait_for_athena_query(athena_client, execution_id, max_execution)
