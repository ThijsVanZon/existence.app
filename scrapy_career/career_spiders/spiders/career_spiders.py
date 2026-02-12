import scrapy


class CareerSpiderIndeed(scrapy.Spider):
    name = 'career_spider_indeed'
    start_urls = ['https://www.indeed.com/jobs?q=software+developer&l=United+States']

    def parse(self, response):
        job_cards = response.css(
            "div.job_seen_beacon, div.slider_container, div[data-testid='jobSeenBeacon']"
        )
        for job in job_cards:
            link = (
                job.css("h2 a::attr(href)").get()
                or job.css("a.jcs-JobTitle::attr(href)").get()
                or job.css("a[data-jk]::attr(href)").get()
            )
            yield {
                'title': (
                    job.css("h2.jobTitle span::text").get()
                    or job.css("a.jcs-JobTitle span::text").get()
                    or job.css("h2 a::text").get()
                ),
                'company': (
                    job.css("[data-testid='company-name']::text").get()
                    or job.css("span.companyName::text").get()
                ),
                'location': (
                    job.css("[data-testid='text-location']::text").get()
                    or job.css("div.companyLocation::text").get()
                ),
                'link': response.urljoin(link) if link else None,
            }


class CareerSpiderLinkedIn(scrapy.Spider):
    name = 'career_spider_linkedin'
    start_urls = [
        'https://www.linkedin.com/jobs/search/?keywords=software%20developer&location=United%20States'
    ]

    def parse(self, response):
        job_cards = response.css("div.base-card, li.base-card, div.result-card")
        for job in job_cards:
            yield {
                'title': (
                    job.css("h3.base-search-card__title::text").get()
                    or job.css("h3.result-card__title::text").get()
                ),
                'company': (
                    job.css("h4.base-search-card__subtitle::text").get()
                    or job.css("h4.result-card__subtitle::text").get()
                ),
                'location': (
                    job.css("span.job-search-card__location::text").get()
                    or job.css("span.job-result-card__location::text").get()
                ),
                'link': (
                    job.css("a.base-card__full-link::attr(href)").get()
                    or job.css("a.result-card__full-card-link::attr(href)").get()
                ),
            }
