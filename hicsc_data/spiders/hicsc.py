import scrapy
import json

class HicscSpider(scrapy.Spider):
    name = 'hicsc'
    allowed_domains = ['hicscdata.hawaii.gov']
    
    def start_requests(self):
        yield scrapy.Request(
            url="https://hicscdata.hawaii.gov/api/id/jexd-xbcg.json?$query=select *, :id where (`date` >= '2021-01-01T00:00:00' and `date` < '2023-01-01T00:00:00') |> select count(*) as __count_alias__&$$read_from_nbe=true&$$version=2.1",
            callback=self.get_count,
            cb_kwargs={'type': 'contribution'}
        )
        yield scrapy.Request(
            url="https://hicscdata.hawaii.gov/api/id/3maa-4fgr.json?$query=select *, :id where (`date` >= '2021-01-01T00:00:00' and `date` <= '2023-01-01T00:00:00') |> select count(*) as __count_alias__&$$read_from_nbe=true&$$version=2.1",
            callback=self.get_count,
            cb_kwargs={'type': 'expenditure'}
        )

    def get_count(self, response, type):
        count = int(json.loads(response.text)[0]['__count_alias__'])
        for offset in range(0, count, 1000):
            if type == 'contribution':
                url="https://hicscdata.hawaii.gov/api/id/jexd-xbcg.json?$query=select *, :id where (`date` >= '2021-01-01T00:00:00' and `date` < '2023-01-01T00:00:00') order by `date` desc offset {offset} limit 1000".format(offset=offset)
            else:
                    
                url="https://hicscdata.hawaii.gov/api/id/3maa-4fgr.json?$query=select *, :id where (`date` >= '2021-01-01T00:00:00' and `date` <= '2023-01-01T00:00:00') order by `date` desc offset {offset} limit 1000".format(offset=offset)
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                cb_kwargs={'type': type}
            )
        

    def parse(self, response, type):
        rows = json.loads(response.text)
        for row in rows:
            row['type']=type
            yield row
