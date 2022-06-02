# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from ast import Mod
from email.policy import default
from itemadapter import ItemAdapter
from peewee import *
import json

db = SqliteDatabase('hw_campaign.db')

class HwCampaignFinanceContribution(Model):
    candidate_name=CharField(null=True, max_length=255)
    contributor_type=CharField(null=True)
    contributor_name=CharField(null=True)
    date=CharField(null=True)
    amount=DecimalField(null=True)
    aggregate=DecimalField(null=True)
    employer=CharField(null=True)
    occupation=CharField(null=True)
    street_address_1=CharField(null=True)
    city=CharField(null=True)
    state=CharField(null=True)
    zip_code=DecimalField(null=True)
    non_resident_yes_or_no_=CharField(null=True)
    non_monetary_yes_or_no=CharField(null=True)
    office=CharField(null=True)
    reg_no=CharField(null=True)
    election_period=CharField(null=True)
    mapping_address=CharField(null=True)
    inoutstate=CharField(null=True)
    iid=CharField(null=True)
    
    class Meta:
        database = db
        table_name = "hw_campaign_finance_contribution"

class HwCampaignFinanceExpenditure(Model):
    candidate_name=CharField(null=True)
    vendor_type=CharField(null=True)
    vendor_name=CharField(null=True)
    date=CharField(null=True)
    amount=DecimalField(null=True)
    expenditure_category=CharField(null=True)
    purpose_of_expenditure=CharField(null=True)
    address_1=CharField(null=True)
    city=CharField(null=True)
    state=CharField(null=True)
    zip_code=CharField(null=True)
    office=CharField(null=True)
    reg_no=CharField(null=True)
    election_period=CharField(null=True)
    inoutstate=CharField(null=True)
    iid=CharField(null=True)

    class Meta:
        database = db
        table_name = "hw_campaign_finance_expenditure"

class HicscDataPipeline:
    def open_spider(self, spider):
        global db
        db.connect()
        db.create_tables([HwCampaignFinanceContribution, HwCampaignFinanceExpenditure])
        self.count = 0

    def process_item(self, item, spider):
        if (item['type'] == 'contribution'):
            HwCampaignFinanceContribution(
                candidate_name=item.get('candidate_name', ""),
                contributor_type=item.get('contributor_type', ""),
                contributor_name=item.get('contributor_name', ""),
                date=item.get('date', ""),
                amount=item.get('amount', ""),
                aggregate=item.get('aggregate', ""),
                employer=item.get('employer', ""),
                occupation=item.get('occupation', ""),
                street_address_1=item.get('street_address_1', ""),
                city=item.get('city', ""),
                state=item.get('state', ""),
                zip_code=item.get('zip_code', ""),
                non_resident_yes_or_no_=item.get('non_resident_yes_or_no_', ""),
                non_monetary_yes_or_no=item.get('non_monetary_yes_or_no', ""),
                office=item.get('office', ""),
                reg_no=item.get('reg_no', ""),
                election_period=item.get('election_period', ""),
                mapping_address=json.dumps(item.get('mapping_address', "")),
                inoutstate=item.get('inoutstate', ""),
                iid=item.get(':id', "")
            ).save()
        else:
            HwCampaignFinanceExpenditure(
                candidate_name=item.get("candidate_name", ""),
                vendor_type=item.get("vendor_type", ""),
                vendor_name=item.get("vendor_name", ""),
                date=item.get("date", ""),
                amount=item.get("amount", ""),
                expenditure_category=item.get("expenditure_category", ""),
                purpose_of_expenditure=item.get("purpose_of_expenditure", ""),
                address_1=item.get("address_1", ""),
                city=item.get("city", ""),
                state=item.get("state", ""),
                zip_code=item.get("zip_code", ""),
                office=item.get("office", ""),
                reg_no=item.get("reg_no", ""),
                election_period=item.get("election_period", ""),
                inoutstate=item.get("inoutstate", ""),
                iid=item.get(":id", "")
            ).save()
        self.count = self.count + 1
        print(self.count)
        return item
    def close_spider(self, spider):
        db.close()
