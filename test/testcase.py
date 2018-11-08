# Attribution: https://github.com/xzkostyan/clickhouse-sqlalchemy
import re

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.dialects import registry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.orm import sessionmaker
from unittest import TestCase

from base import dialect

registry.register("clickhouse", "base", "dialect")
engine = create_engine('clickhouse://default:@ch-0/default')
session = sessionmaker(bind=engine)()
metadata = MetaData(bind=engine)
Base = declarative_base(metadata=metadata)

class BaseTestCase(TestCase):
    strip_spaces = re.compile(r'[\n\t]')

    def _compile(self, clause, literal_binds=False):
        if isinstance(clause, Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        kw = {}
        if literal_binds:
            kw['compile_kwargs'] = {}
            kw['compile_kwargs']['literal_binds'] = True

        return clause.compile(dialect=dialect(), **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )
