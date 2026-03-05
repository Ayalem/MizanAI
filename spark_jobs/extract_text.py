from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
import fitz
from pyspark.sql.types import ArrayType
import re
import hashlib
import psycopg2
# .config(\\\):load JDBCdriver to be able to communicate with postgres 
#function definitions 


# 1-extracting legal texts using fitz
def extract_text(filepath):
    try:
        doc=fitz.open(filepath)
        text=""
        for page in doc:
            text+=page.get_text(flags=fitz.TEXT_PRESERVE_LIGATURES | fitz.TEXT_PRESERVE_WHITESPACE)
        doc.close()
        return text.strip() if text.strip() else None
    except Exception as e:
        return f"Error:{str(e)}"


#2-clean the text

def clean_text(text,language):
    if not text:
        return None
    
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text) 
    text = text.strip()


    if language=="ar":
        text = re.sub(r'[\u0610-\u061A\u064B-\u065F]', '', text)
        text = text.replace("أ", "ا")
        text = text.replace("إ", "ا")
        text = text.replace("آ", "ا")  
        text = re.sub(r'\u0640', '', text)
    elif language=="fr":
      text = text.replace("'", "'") 
      text=text.replace("«","'")
      text=text.replace("»","'")
    return text 

#3-chunking the texts after doing embeddings 
def chunk_text(text):
    if not text:
        return []

    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained("intfloat/multilingual-e5-large")
    tokens=tokenizer.encode(text)
    chunks=[]
    chunk_size=400
    overlap=80
    start=0
    while start<len(tokens):
        end=start+chunk_size
        chunk_tokens=tokens[start:end]
        chunk_text=tokenizer.decode(chunk_tokens,skip_special_tokens=True)
        chunks.append(chunk_text)
        start+=chunk_size-overlap
    return chunks
#4- saving to postgres 

def save_chunks(row):
    conn=psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor=conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chunks(
         id SERIAL PRIMARY KEY ,
         chunk_id  TEXT UNIQUE ,
         doc_id TEXT,
         chunk_index INT,
         text TEXT,
         language TEXT,
         source_type TEXT,
         created_at TIMESTAMP DEFAULT NOW())
    """)

    for i,chunk in enumerate(row.chunks):
        chunk_id=hashlib.sha256(f"{row.id}_{i}".encode()).hexdigest()
        cursor.execute("""
        INSERT INTO chunks(chunk_id,doc_id,chunk_index,text,language,source_type)
        VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT (chunk_id) DO NOTHING
        """,(chunk_id,str(row.id),i,chunk,row.language,row.category))
    conn.commit()
    cursor.close()
    conn.close()




#STARTING SPARK SESSION
spark=SparkSession.builder\
    .appName("LagalDocumentTextExtractor") \
    .config("spark.jars","/opt/spark_jobs/postgresql-42.7.3.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("Spark session started")




#READ DATA FROM POSTGRES
df=spark.read.format("jdbc")\
    .option("url","jdbc:postgresql://postgres:5432/airflow")\
    .option("dbtable","law_documents")\
    .option("user","airflow")\
    .option("password","airflow")\
    .option("driver", "org.postgresql.Driver")\
    .load()

print(f"Documents found: {df.count()}")

df.show()



#A-APPLY EXTRACTION TEXT
extract_text_udf=udf(extract_text,StringType())
df_with_text=df.withColumn("raw_text",extract_text_udf(col("filepath")))
df_with_text=df_with_text.filter(col("raw_text").isNotNull())

print(f"Documents with text:{df_with_text.count()}")
df_with_text.select("title", "raw_text").show(5, truncate=50)



#B-APPLY CLEANING TEXT
clean_text_udf=udf(clean_text,StringType())
df_cleaned=df_with_text.withColumn("cleaned_text",clean_text_udf(col("raw_text"),col("language")))



#C-APPLY CHUNKING TEXT
chunk_udf=udf(chunk_text,ArrayType(StringType()))
df_chunked=df_cleaned.withColumn("chunks",chunk_udf(col("cleaned_text")))
print("Chunking done")
df_chunked.select("title", "chunks").show(3, truncate=50)


#D-APPLY SAVING TO POSTGRES
df_chunked.foreach(save_chunks)
print("Chunks saved to PostgreSQL")




spark.stop()