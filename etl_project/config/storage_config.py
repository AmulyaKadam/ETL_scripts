
def configure_adls(spark):
    """
    Configure spark.conf for ADLS OAuth access using dbutils.secrets.
    Assumes a Databricks secret scope named 'my_scope' with keys:
      - adls_client_id
      - adls_client_secret
      - adls_tenant_id
    Replace scope/key names and account name as necessary.
    """
    adls_account_name = "mydatalake"  

    
    client_id = dbutils.secrets.get(scope="my_scope", key="adls_client_id")
    client_secret = dbutils.secrets.get(scope="my_scope", key="adls_client_secret")
    tenant_id = dbutils.secrets.get(scope="my_scope", key="adls_tenant_id")

    configs = {
        f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net": "OAuth",
        f"fs.azure.account.oauth.provider.type.{adls_account_name}.dfs.core.windows.net":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{adls_account_name}.dfs.core.windows.net": client_id,
        f"fs.azure.account.oauth2.client.secret.{adls_account_name}.dfs.core.windows.net": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{adls_account_name}.dfs.core.windows.net":
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    for k, v in configs.items():
        spark.conf.set(k, v)
