from datetime import datetime, timedelta
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
            result = await self._parse_result(pipeline, query)
            return result
        except ValidationError as e:
            return traceback.format_exc()
        except Exception as e:
            return traceback.format_exc()

    def _get_pipeline(self, query: Query):
        group_fields = { "year": { "$year": "$dt" },
                        "month": { "$month": "$dt" }}
        sort_fields = [("_id.year", 1), ("_id.month", 1)]
        if query.group_type == GroupByEnum.Month:
            pass
        else:
            group_fields.update({ "day": { "$dayOfMonth": "$dt" } } )
            sort_fields.append(("_id.day", 1))
            if query.group_type  == GroupByEnum.Hour:
                group_fields.update({ "hour": { "$hour": "$dt" } } )
                sort_fields.append(("_id.hour", 1))

        return [{ "$match": { "dt": {"$gte": query.dt_from, "$lte": query.dt_upto } }}, 
            { "$group": { "_id": group_fields, "totalAmount": { "$sum": "$value" }}},
            {"$sort": SON(sort_fields)}]
    
    def _next(self, date, group_type):
        if group_type == GroupByEnum.Month:
            year = date.year + (date.month == 12)
            month = date.month % 12 + 1
            return datetime(year=year, month=month, day=1)
        elif group_type == GroupByEnum.Day:
            unit = timedelta(seconds=3600 * 24)
            date += unit
            return datetime(year=date.year, month=date.month, day=date.day)
        elif group_type == GroupByEnum.Hour:
            unit = timedelta(seconds=3600)
            date += unit
            return datetime(year=date.year, month=date.month, day=date.day, hour=date.hour)
        else:
            raise ValueError

    def _delta(self, cur_date, prev_date, group_type):

        labels = []
        prev_date = self._next(prev_date, group_type)
        while prev_date < cur_date: 
            labels.append(prev_date.strftime(TIME_FMT))
            prev_date = self._next(prev_date, group_type)
                
        return labels
    
    async def _parse_result(self, pipeline, query):
        labels = []
        dataset = []
        result = {"dataset": dataset, "labels": labels}
        prev_date = query.dt_from
        last_date = query.dt_upto
        group_type = query.group_type

        async for r in self.collection.aggregate(pipeline=pipeline):
            cur_date = datetime(**r['_id'])
            
            delta_labels = self._delta(cur_date, prev_date, group_type)
            labels.extend(delta_labels)
            dataset.extend([0] * len(delta_labels))
            
            prev_date = cur_date
            labels.append(cur_date.strftime(TIME_FMT))
            dataset.append(r['totalAmount'])
        
        # прибавить в начало 0, если первый платеж не в первый отрезок
        first_date = datetime.strptime(dataset[0], TIME_FMT)
        if first_date > query.dt_from:
            delta_labels = self._delta(first_date, query.dt_from, group_type)
            num = len(delta_labels)
            delta_labels.extend(labels)
            dataset = ([0] * num).extend(dataset)

        # прибавить в конец 0, если последний платеж не в последний отрезок
        if prev_date < last_date:
            delta_labels = self._delta(self._next(last_date, group_type), prev_date, group_type)
            labels.extend(delta_labels)
            dataset.extend([0] * len(delta_labels))

        return json.dumps(result)
