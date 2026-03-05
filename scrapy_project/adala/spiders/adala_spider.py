import scrapy 
from adala.items import LawDocumentItem
class AdalaSpider(scrapy.Spider):
    name="adala"
    CATEGORIES={
    # Arabic 
    "civil_ar": {
        "url": "https://adala.justice.gov.ma/resources/1052",
        "language": "ar"
    },
    # French 
    "famille_fr": {
        "url": "https://adala.justice.gov.ma/fr/resources/433",
        "language": "fr"
    },
    "penal_fr": {
        "url": "https://adala.justice.gov.ma/fr/resources/424",
        "language": "fr"
    },
    "travail_fr": {
        "url": "https://adala.justice.gov.ma/fr/resources/420",
        "language": "fr"
    },

    }
    #mapping categories arabic
    ARABIC_TITLE_TO_CATEGORY={
        "قانون المسطرة المدنية":"civil",
         "قانون بمثابة مدونة الأسرة":"famille",
        "القانون المتعلق بمدونة الشغل":"travail",
    "ظهير بمثابة قانون الالتزامات والعقود": "civil",
        "قانون يتعلق بمدونة التجارة":  "commercial",
      "القانون المتعلق بمدونة الحقوق العينية":   "civil",

    }

    def parse(self,response):
        language=response.meta["language"]
        for link in response.css('a[href*="api/uploads/"]'):
            title=link.css('::text').get("").strip()
            href=link.attrib["href"].split("#")[0]
            pdf_url=response.urljoin(href)
        
            if language == "ar":
                category = self.ARABIC_TITLE_TO_CATEGORY.get(title, "general")
            else:
                category=response.meta["category"]
            
            yield LawDocumentItem(
                title=title,
                pdf_url=pdf_url,
                category=category,
                language=language
            )
    def start_requests(self):
        for category_name,config in self.CATEGORIES.items():
               yield scrapy.Request(
                 url=config["url"],
                 callback=self.parse,
                 meta={
                  "category":category_name,
                    "language":config["language"],
                 }
        )     
    
