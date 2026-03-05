BOT_name="adala"
SPIDER_MODULES=['adala.spiders']
NEWSPIDER_MODULE='adala.spiders'
#rate limiting 
DOWNLOAD_DELAY=1
RANDOMIZE_DOWNLOAD_DELAY=True
CONCURRENT_REQUESTS=8
#retries
RETRY_TIMES=3
RETRY_HTTP_CODES=[403,429,500,502,503,504]
#logging
LOG_LEVEL='INFO'
LOG_FILE='logs/adala.log'
#pdf downloads
FILES_STORE="opt/airflow/data/raw/laws"
#item pipelines:execution order

ITEM_PIPELINES={
    "adala.pipelines.PDFDownloadPipeline":100, #first we start by downloading
    "adala.pipelines.PostgresSQLPipeline":200 #then we store metadata in postgres
}
POSTGRES_HOST="postgres"
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"
POSTGRES_DB="airflow"
