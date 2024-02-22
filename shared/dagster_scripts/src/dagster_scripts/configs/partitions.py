import datetime as dt
import typing
from abc import ABC, abstractmethod

import pendulum
from pydantic import BaseModel


class PartitionConfig(BaseModel, ABC):
    name: str
    values: typing.Any

    @abstractmethod
    def config_dict(self):
        pass


class DateRangeConfig(BaseModel):
    start_date: dt.date
    end_date: dt.date


class DatePartitionConfig(PartitionConfig):
    values: DateRangeConfig

    def _get_range(self):
        return pendulum.period(self.values.start_date, self.values.end_date).range("days")

    def config_dict(self):
        return [{f"dagster/partition/{self.name}": d.to_date_string()} for d in self._get_range()]


class StaticPartitionConfig(PartitionConfig):
    values: typing.List[str]

    def config_dict(self):
        return [{f"dagster/partition/{self.name}": v} for v in self.values]
