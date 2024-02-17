import typing
import datetime as dt

import pendulum
from pydantic import BaseModel

#%% Configuration

class DateBackfillPartitionConfig(BaseModel):
	name: str
	start_date: dt.date
	end_date: dt.date

	def get_range(self):
		return pendulum.period(self.start_date, self.end_date).range('days')

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": d.to_date_string()} for d in self.get_range()]


class StaticBackfillPartitionConfig(BaseModel):
	name: str
	values: typing.List[str]

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": v} for v in self.values]


class BackfillTagsConfig(BaseModel):
	name: str
	partitions: typing.List[typing.Union[StaticBackfillPartitionConfig, DateBackfillPartitionConfig]]


class BackfillConfig(BaseModel):
	job_name: str
	repository_name: str
	tags: BackfillTagsConfig
