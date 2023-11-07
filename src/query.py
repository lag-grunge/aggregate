from datetime import datetime
import json
import motor.motor_asyncio
from bson.son import SON
from enum import Enum
from pydantic import BaseModel, ValidationError
import traceback

class GroupByEnum(str, Enum):
    Hour = "hour"
    Day = "day"
    Month = "month"

TIME_FMT='%Y-%m-%dT%H:%M:%S'

class Query(BaseModel):
    dt_from: datetime
    dt_upto: datetime
    group_type: GroupByEnum

class Aggregator:
    def __init__(self, uri: str, db: str, collection: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self.client[db]
        self.collection = self.db[collection]
    
    async def aggregate(self, query: str):
        try:
            query = json.loads(query)
            query = Query(**query)
            pipeline = self._get_pipeline(query)
            result = await self._parse_result(pipeline)
            return result
        except ValidationError as e:
            return traceback.format_exc()
        except Exception as e:
            return traceback.format_exc()

    def _get_pipeline(self, query: Query):
        group_fields = { "year": { "$year": "$dt" },
                        "month": { "$month": "$dt" }}
        sort_fields = [("_id.year", 1), ("_id.month", 1)]
        if query.group_type < GroupByEnum.Month:
            group_fields.update({ "day": { "$dayOfMonth": "$dt" } } )
            sort_fields.append(("_id.day", 1))
        if query.group_type < GroupByEnum.Day:
            group_fields.update({ "hour": { "$hour": "$dt" } } )
            sort_fields.append(("_id.hour", 1))

        return [{ "$match": { "dt": {"$gt": query.dt_from, "$lt": query.dt_upto } }}, 
            { "$group": { "_id": group_fields, "totalAmount": { "$sum": "$value" }}},
            {"$sort": SON(sort_fields)}]
    
    async def _parse_result(self, pipeline):
        labels = []
        dataset = []
        result = {"dataset": dataset, "labels": labels}
        async for r in self.collection.aggregate(pipeline=pipeline):
            labels.append(datetime(**r['_id']).strftime(TIME_FMT))
            dataset.append(r['totalAmount'])
        return json.dumps(result)
