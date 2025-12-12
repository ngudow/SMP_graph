from neo4j import GraphDatabase
import logging

class Neo4jConnection:
    def __init__(self, uri, user, password, logger=None):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._logger = logger or logging.getLogger(__name__)

    def close(self):
        if self._driver:
            self._driver.close()

    def run(self, query, parameters=None, db=None):
        try:
            with self._driver.session(database=db) as session:
                result = session.run(query, parameters)
                return [dict(record) for record in result]
        except Exception as e:
            self._logger.exception("Neo4j query failed")
            raise

    def run_single(self, query, parameters=None, db=None):
        rows = self.run(query, parameters, db=db)
        return rows[0] if rows else None
