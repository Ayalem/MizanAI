import os
import hashlib
import psycopg2
import requests
from datetime import datetime
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class PDFDownloadPipeline:
    def process_item(self,item,spider):
        adapter=ItemAdapter(item)
        pdf_url=adapter.get("pdf_url")
        category=adapter.get("category")
        if not pdf_url:
            raise DropItem("Missing PDF URL")
        if not category:
            raise DropItem("Missing category")
        
        save_dir=os.path.join("/opt/airflow/data/raw/laws",category)
        os.makedirs(save_dir,exist_ok=True)
        #clean filename from title

        title=adapter.get("title","unknown")
        safe_title="".join(c for c in title if c.isalnum() or c in (" ","-","_"))[:80]
        filename=f"{safe_title}.pdf"
        filepath=os.path.join(save_dir,filename)


        #if already downloaded:
        if os.path.exists(filepath):
            spider.logger.info(f"PDF already exists:{filepath}")
            adapter["filepath"]=filepath
            return item
        #download pdf

        response=requests.get(pdf_url,timeout=60)
        if response.status_code!=200:
            raise DropItem(f"Failed to download PDF:{response.status_code}')")
        
        with open(filepath,"wb") as f:#write binary in memory 
            f.write(response.content)#content is the  bytes of pdf when we used get

          # fill remaining fields
        adapter["filepath"]     = filepath
        adapter["file_hash"]    = hashlib.sha256(response.content).hexdigest()
        adapter["file_size_kb"] = round(len(response.content) / 1024, 2)
        adapter["downloaded_at"] = datetime.now().isoformat()

        spider.logger.info(f"Downloaded: {filename}")
        return item


class PostgresSQLPipeline :



    def open_spider(self,spider):
        self.conn=psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow")

        self.cursor=self.conn.cursor()
        self._create_table()
    
    def _create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS law_documents(
            id SERIAL PRIMARY KEY,
            title         TEXT,
            pdf_url         TEXT UNIQUE, 
            category        TEXT,
            language        TEXT,
            filepath        TEXT,
            file_hash       TEXT,
            file_size_kb    TEXT,
            downloaded_at TIMESTAMP,
            created_at  TIMESTAMP DEFAULT NOW()

        )
        """)
        self.conn.commit()

    def process_item(self,item,spider):
        adapter=ItemAdapter(item)
        self.cursor.execute(""" 
        INSERT INTO law_documents(title,pdf_url,category,language,filepath,file_hash,file_size_kb,downloaded_at) 
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(pdf_url) DO NOTHING""",(
            adapter.get("title"),
            adapter.get("pdf_url"),
            adapter.get("category"),
            adapter.get("language"),
            adapter.get("filepath"),
            adapter.get("file_hash"),
            adapter.get("file_size_kb"),
            adapter.get("downloaded_at")
          ))
    
        self.conn.commit()
        return item
    
    def close_spider(self,spider):
        self.cursor.close()
        self.conn.close()
        spider.logger.info("Pipeline closed")
         

    


