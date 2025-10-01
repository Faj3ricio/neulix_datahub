from .base_spider import BaseSpider

class WikipediaSpider(BaseSpider):
    def run(self):
        self.driver.get('https://en.wikipedia.org/wiki/Main_Page')
        title = self.driver.title
        print(f"Wikipedia Title: {title}")
        self.close()
