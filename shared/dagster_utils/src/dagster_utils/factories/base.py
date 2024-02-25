import abc
import typing


class DagsterObjectFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(
        self,
        name: str,
        description: typing.Optional[str] = None,
    ):
        self.name = name
        self.description = description

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        pass
