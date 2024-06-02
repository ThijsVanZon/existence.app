import scrapy

class CareerSpiderIndeed(scrapy.Spider):
    name = 'career_spider_indeed'
    start_urls = ['https://www.indeed.com/jobs?q=developer&l=']

    def parse(self, response):
        for job in response.css('div.jobsearch-SerpJobCard'):
            yield {
                'title': job.css('a.jobtitle::text').get(),
                'company': job.css('span.company::text').get(),
                'location': job.css('div.location::text').get(),
                'link': job.css('a.jobtitle::attr(href)').get()
            }

class CareerSpiderLinkedIn(scrapy.Spider):
    name = 'career_spider_linkedin'
    start_urls = ['https://www.linkedin.com/jobs/search/?keywords=developer']

    def parse(self, response):
        for job in response.css('div.result-card'):
            yield {
                'title': job.css('h3.result-card__title::text').get(),
                'company': job.css('h4.result-card__subtitle::text').get(),
                'location': job.css('span.job-result-card__location::text').get(),
                'link': job.css('a.result-card__full-card-link::attr(href)').get()
            }
