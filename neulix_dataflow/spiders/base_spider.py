from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from abc import ABC, abstractmethod

class BaseSpider(ABC):
    def __init__(self, headless=True):
        options = Options()
        options.headless = headless
        self.driver = webdriver.Firefox(options=options)

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def close(self):
        self.driver.quit()
