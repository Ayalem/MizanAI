import scrapy 
class LawDocumentItem(scrapy.Item):
    title=scrapy.Field()
    pdf_url=scrapy.Field()
    category=scrapy.Field()
    language=scrapy.Field()
    filepath=scrapy.Field()
    downloaded_at=scrapy.Field()
    file_hash=scrapy.Field()
    file_size_kb= scrapy.Field() 
    num_pages= scrapy.Field()
