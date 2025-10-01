from .base_spider import BaseSpider

class LinkedInSpider(BaseSpider):
    def run(self):
        self.driver.get('https://www.linkedin.com/login')
        title = self.driver.title
        print(f"LinkedIn Title: {title}")
        self.close()
