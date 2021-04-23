import re
import json
from typing import Any
from datetime import datetime

from sqlalchemy import Column, DateTime, String, Text, Integer
from sqlalchemy.types import TypeDecorator
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.declarative import as_declarative, declared_attr

from ontelligence.utils.date import tzware_datetime


########################################################################################################################
# Custom data types.
########################################################################################################################


class AwareDateTime(TypeDecorator):
    """A DateTime type which can only store tz-aware DateTimes"""

    impl = DateTime(timezone=True)

    def __repr__(self):
        return 'AwareDateTime()'

    def process_bind_param(self, value, dialect):
        if isinstance(value, datetime) and value.tzinfo is None:
            raise ValueError('{!r} must be TZ-aware'.format(value))
        return value


class JsonEncodedDict(TypeDecorator):
    """A JSON object type which automatically serializes and deserializes values"""

    impl = Text

    def process_bind_param(self, value, dialect):
        return json.dumps(value) if value is not None else {}

    def process_result_value(self, value, dialect):
        return json.loads(value) if value is not None else {}


MutableDict.associate_with(JsonEncodedDict)


########################################################################################################################
# Base models and mixins.
########################################################################################################################


@as_declarative()
class BaseModel:
    id: Any
    __name__: str

    @declared_attr
    def __tablename__(cls) -> str:
        """Converts class name from camel case to snake case"""
        return re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()


class ResourceMixin:

    # Keep track when records are created and updated.
    created_on = Column(AwareDateTime(), default=tzware_datetime)
    updated_on = Column(AwareDateTime(), default=tzware_datetime, onupdate=tzware_datetime)

    def __str__(self):
        """Create a human readable version of a class instance"""
        obj_id = hex(id(self))
        columns = self.__table__.c.keys()
        values = ', '.join("%s=%r" % (n, getattr(self, n)) for n in columns)
        return '<%s %s(%s)>' % (obj_id, self.__class__.__name__, values)


class UserMixin(ResourceMixin):

    # Activity tracking.
    sign_in_count = Column(Integer, nullable=False, default=0)
    current_sign_in_on = Column(AwareDateTime())
    current_sign_in_ip = Column(String(45))
    last_sign_in_on = Column(AwareDateTime())
    last_sign_in_ip = Column(String(45))
