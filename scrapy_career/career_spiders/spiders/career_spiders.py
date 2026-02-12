import scrapy
from urllib.parse import quote_plus


SLEEVE_QUERIES = {
    "A": [
        ("av technician", "Netherlands"),
        ("event technician", "Europe"),
        ("production technician", "Netherlands"),
        ("lighting technician", "Europe"),
    ],
    "B": [
        ("solutions engineer", "Remote"),
        ("product operations", "Europe"),
        ("workflow automation", "Remote"),
        ("implementation consultant", "Netherlands"),
    ],
    "C": [
        ("creative producer", "Europe"),
        ("experience design", "Netherlands"),
        ("immersive production", "Europe"),
        ("brand activation producer", "Netherlands"),
    ],
    "D": [
        ("field service engineer", "Netherlands"),
        ("technical operations", "Europe"),
        ("commissioning engineer", "Europe"),
        ("onsite technical support", "Netherlands"),
    ],
    "E": [
        ("community manager events", "Europe"),
        ("partnership manager events", "Netherlands"),
        ("festival partnerships", "Europe"),
        ("event marketing coordinator", "Netherlands"),
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
