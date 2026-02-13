import scrapy
from urllib.parse import quote_plus


SLEEVE_QUERIES = {
    "A": [
        ("av technician", "Netherlands"),
        ("audiovisual technician", "Netherlands"),
        ("show technician", "Europe"),
        ("venue technician", "Europe"),
        ("live sound technician", "Netherlands"),
        ("lighting technician", "Europe"),
        ("video technician", "Europe"),
        ("stagehand", "Netherlands"),
    ],
    "B": [
        ("implementation consultant", "Europe"),
        ("solutions engineer workflow", "Europe"),
        ("workflow automation", "Europe"),
        ("product operations", "Europe"),
        ("integrations specialist", "Netherlands"),
        ("business operations specialist", "Europe"),
        ("systems analyst", "Europe"),
        ("revops", "Europe"),
    ],
    "C": [
        ("technical producer", "Europe"),
        ("creative producer", "Europe"),
        ("experience producer", "Europe"),
        ("production coordinator events", "Netherlands"),
        ("creative technologist", "Europe"),
        ("immersive producer", "Europe"),
        ("exhibition producer", "Europe"),
        ("event producer", "Netherlands"),
    ],
    "D": [
        ("field service engineer", "Netherlands"),
        ("service technician", "Netherlands"),
        ("commissioning engineer", "Europe"),
        ("installation engineer", "Netherlands"),
        ("inbedrijfstelling engineer", "Netherlands"),
        ("on-site support engineer", "Europe"),
        ("systems integrator technician", "Europe"),
        ("service engineer", "Netherlands"),
    ],
    "E": [
        ("partnerships manager events", "Europe"),
        ("community manager culture", "Europe"),
        ("program coordinator events", "Netherlands"),
        ("sponsorship manager festival", "Europe"),
        ("artist relations", "Europe"),
        ("talent buyer", "Netherlands"),
        ("booker", "Netherlands"),
        ("event marketing manager", "Europe"),
    ],
}

def sleeve_queries_for(sleeve_key):
    key = (sleeve_key or "").upper()
    return SLEEVE_QUERIES.get(key, [])


class CareerSpiderIndeed(scrapy.Spider):
    name = "career_spider_indeed"

    def __init__(self, sleeve_key=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleeve_key = (sleeve_key or "").upper()

    def start_requests(self):
        for term, location in sleeve_queries_for(self.sleeve_key):
            term_q = quote_plus(term)
            loc_q = quote_plus(location)
            url = f"https://www.indeed.com/jobs?q={term_q}&l={loc_q}"
            yield scrapy.Request(url=url, callback=self.parse)

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
            snippet = (
                job.css("div.job-snippet::text").getall()
                or job.css("[data-testid='text-snippet']::text").getall()
            )
            work_mode_hint = " ".join(snippet) if snippet else ""

            yield {
                "title": (
                    job.css("h2.jobTitle span::text").get()
                    or job.css("a.jcs-JobTitle span::text").get()
                    or job.css("h2 a::text").get()
                ),
                "company": (
                    job.css("[data-testid='company-name']::text").get()
                    or job.css("span.companyName::text").get()
                ),
                "location": (
                    job.css("[data-testid='text-location']::text").get()
                    or job.css("div.companyLocation::text").get()
                ),
                "link": response.urljoin(link) if link else None,
                "snippet": " ".join(part.strip() for part in snippet if part.strip()),
                "salary": (
                    job.css("span.salary-snippet::text").get()
                    or job.css("[data-testid='attribute_snippet_testid']::text").get()
                ),
                "date": (
                    job.css("span.date::text").get()
                    or job.css("span[data-testid='myJobsStateDate']::text").get()
                ),
                "work_mode_hint": work_mode_hint,
                "source": "Indeed",
            }


class CareerSpiderLinkedIn(scrapy.Spider):
    name = "career_spider_linkedin"

    def __init__(self, sleeve_key=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleeve_key = (sleeve_key or "").upper()

    def start_requests(self):
        for term, location in sleeve_queries_for(self.sleeve_key):
            term_q = quote_plus(term)
            loc_q = quote_plus(location)
            url = (
                "https://www.linkedin.com/jobs/search/"
                f"?keywords={term_q}&location={loc_q}"
            )
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        job_cards = response.css("div.base-card, li.base-card, div.result-card")
        for job in job_cards:
            metadata = " ".join(
                job.css(
                    "div.base-search-card__metadata *::text, "
                    "ul.job-search-card__job-insight *::text, "
                    "div.result-card__meta *::text"
                ).getall()
            )
            yield {
                "title": (
                    job.css("h3.base-search-card__title::text").get()
                    or job.css("h3.result-card__title::text").get()
                ),
                "company": (
                    job.css("h4.base-search-card__subtitle::text").get()
                    or job.css("h4.result-card__subtitle::text").get()
                ),
                "location": (
                    job.css("span.job-search-card__location::text").get()
                    or job.css("span.job-result-card__location::text").get()
                ),
                "link": (
                    job.css("a.base-card__full-link::attr(href)").get()
                    or job.css("a.result-card__full-card-link::attr(href)").get()
                ),
                "snippet": metadata,
                "salary": None,
                "date": (
                    job.css("time::attr(datetime)").get()
                    or job.css("time::text").get()
                ),
                "work_mode_hint": metadata,
                "source": "LinkedIn",
            }
