from config.aws import client

def call_extract_glue_job():
    """
    Chama o job extract no Glue.
    """
    lambda_client = client('lambda')

    lambda_client.invoke(
        FunctionName='CallExtractGlueJob',
        InvocationType='Event',
    )

    print("Função lambda CallExtractGlueJob chamada com sucesso.")
