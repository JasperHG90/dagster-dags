import typing
import datetime as dt
from abc import ABC, abstractmethod

import pendulum
from pydantic import BaseModel


class PartitionConfig(BaseModel, ABC):
    name: str

    @abstractmethod
    def config_dict(self):
        pass


class DatePartitionConfig(PartitionConfig):
	start_date: dt.date
	end_date: dt.date

	def _get_range(self):
		return pendulum.period(self.start_date, self.end_date).range('days')

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": d.to_date_string()} for d in self._get_range()]


class StaticPartitionConfig(PartitionConfig):
	values: typing.List[str]

	def config_dict(self):
		return [{f"dagster/partition/{self.name}": v} for v in self.values]
