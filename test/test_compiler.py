from sqlalchemy import column, func, Integer, select, String, table
from sqlalchemy.exc import CompileError
from sqlalchemy import Column, Integer, String

from test.testcase import Base, BaseTestCase, session

tbl = table(
    'nba',
    column('id', Integer),
    column('name', String),
    column('city', String),
    column('wins', Integer)
)

class Nba(Base):
  id = Column(Integer, primary_key=True)
  name = Column(String)
  city = Column(String)
  wins = Column(Integer)
  __tablename__ = 'nba'

class GroupBySummariesTestCase(BaseTestCase):
    def test_group_by_without_summary(self):
        stmt = select([tbl.c.id, tbl.c.name, tbl.c.city]).group_by(
                  tbl.c.id, tbl.c.name, tbl.c.city)
        self.assertEqual(
            self.compile(stmt),
            'SELECT id, name, city FROM nba GROUP BY id, name, city'
        )

    def test_group_by_without_summary_with_labels(self):
        stmt = select([
            tbl.c.name.label('labeled_name'), tbl.c.city.label('labeled_city')
        ]).group_by('labeled_name', 'labeled_city')
        self.assertEqual(
            self.compile(stmt),
            'SELECT name AS labeled_name, city AS labeled_city FROM nba GROUP BY labeled_name, labeled_city'
        )

    def test_group_by_with_rollup(self):
        stmt = select([tbl.c.id]).group_by(tbl.c.id, func.with_rollup())
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY id WITH ROLLUP'
        )

    def test_group_by_with_rollup_with_totals(self):
        stmt = select([tbl.c.id]).group_by(
          tbl.c.id, func.with_rollup(), func.with_totals()
        )
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY id WITH ROLLUP WITH TOTALS'
        )

    def test_group_by_rollup(self):
        stmt = select([tbl.c.id]).group_by(
          func.rollup(tbl.c.id, tbl.c.name)
        )
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY ROLLUP(id, name)'
        )

    def test_group_by_cube(self):
      stmt = select([tbl.c.id, tbl.c.name, tbl.c.city]).group_by(
        func.cube(tbl.c.id, tbl.c.name, tbl.c.city)
      )
      self.assertEqual(
          self.compile(stmt),
          'SELECT id, name, city FROM nba GROUP BY CUBE(id, name, city)'
      )

    def test_group_by_with_cube_with_totals(self):
      stmt = select(
        [tbl.c.id, tbl.c.name, tbl.c.city, func.SUM(tbl.c.wins).label('wins')]
      ).group_by(
        tbl.c.id, tbl.c.name, tbl.c.city, func.with_cube(), func.with_totals()
      )
      self.assertEqual(
          self.compile(stmt),
          'SELECT id, name, city, SUM(wins) AS wins FROM nba GROUP BY id, name,'
          ' city WITH CUBE WITH TOTALS'
      )

class LimitClauseTestCase(BaseTestCase):
    def test_limit(self):
        stmt = select([tbl.c.id, tbl.c.name]).limit(10)
        self.assertEqual(
            self.compile(stmt, literal_binds=True),
            'SELECT id, name FROM nba  LIMIT 10'
        )

    def test_limit_with_offset(self):
        stmt = select([tbl.c.id, tbl.c.name]).limit(10).offset(5)
        self.assertEqual(
            self.compile(stmt, literal_binds=True),
            'SELECT id, name FROM nba  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        stmt = select([tbl.c.id, tbl.c.name]).offset(5)
        with self.assertRaises(CompileError) as ctx:
            self.compile(stmt, literal_binds=True)


class GroupBySummariesDeclarativeTestCase(BaseTestCase):
    def test_group_by_without_summary(self):
        query = session.query(Nba.id, Nba.name, Nba.city).group_by(
                Nba.id, Nba.name, Nba.city)
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id, name AS nba_name, city AS nba_city FROM nba GROUP BY id, name, city'
        )

    def test_group_by_without_summary_with_labels(self):
        query = session.query(
            Nba.name.label('labeled_name'), Nba.city.label('labeled_city')
        ).group_by('labeled_name', 'labeled_city')
        self.assertEqual(
            self.compile(query),
            'SELECT name AS labeled_name, city AS labeled_city FROM nba GROUP BY labeled_name, labeled_city'
        )

    def test_group_by_with_rollup(self):
        query = session.query(Nba.id).group_by(Nba.id, func.with_rollup())
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id FROM nba GROUP BY id WITH ROLLUP'
        )

    def test_group_by_with_rollup_with_totals(self):
        query = session.query(Nba.id).group_by(
            Nba.id, func.with_rollup(), func.with_totals()
        )
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id FROM nba GROUP BY id WITH ROLLUP WITH TOTALS'
        )

    def test_group_by_rollup(self):
        query = session.query(Nba.id).group_by(func.rollup(Nba.id, Nba.name))
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id FROM nba GROUP BY ROLLUP(id, name)'
        )

    def test_group_by_cube(self):
        query = session.query(Nba.id, Nba.name, Nba.city).group_by(
            func.cube(Nba.id, Nba.name, Nba.city)
        )
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id, name AS nba_name, city AS nba_city FROM nba GROUP BY CUBE(id, name, city)'
        )

    def test_group_by_with_cube_with_totals(self):
        query = session.query(
            Nba.id, Nba.name, Nba.city, func.SUM(Nba.wins).label('wins')
        ).group_by(
            Nba.id, Nba.name, Nba.city, func.with_cube(), func.with_totals()
        )
        self.assertEqual(
            self.compile(query),
            'SELECT id AS nba_id, name AS nba_name, city AS nba_city, SUM(wins) AS wins FROM nba GROUP BY id, name, city WITH CUBE WITH TOTALS'
        )

class LimitClauseDeclarativeTestCase(BaseTestCase):
    def test_limit(self):
        query = session.query(Nba.id, Nba.name).limit(10)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT id AS nba_id, name AS nba_name FROM nba  LIMIT 10'
        )

    def test_limit_with_offset(self):
        query = session.query(Nba.id, Nba.name).limit(10).offset(5)
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT id AS nba_id, name AS nba_name FROM nba  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        query = session.query(Nba.id, Nba.name).offset(5)
        with self.assertRaises(CompileError) as ctx:
            self.compile(query, literal_binds=True)
