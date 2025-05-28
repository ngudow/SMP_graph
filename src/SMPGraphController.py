from neo4j import GraphDatabase
from datetime import datetime
import logging

class InvestmentGraphController:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._logger = logging.getLogger(__name__)

    def close(self):
        self._driver.close()

    def _execute_query(self, query, parameters=None):
        with self._driver.session() as session:
            try:
                result = session.run(query, parameters)
                return [dict(record) for record in result]
            except Exception as e:
                self._logger.error(f"Query failed: {e}")
                return None

    # --- Основные методы для работы с данными ---

    def create_user(self, user_id, risk_tolerance="moderate", investment_horizon="medium"):
        query = """
        MERGE (u:User {id: $user_id})
        SET u.risk_tolerance = $risk_tolerance,
            u.investment_horizon = $investment_horizon
        RETURN u
        """
        return self._execute_query(query, {
            "user_id": user_id,
            "risk_tolerance": risk_tolerance,
            "investment_horizon": investment_horizon
        })

    def add_stock(self, ticker, company_name, sector):
        query = """
        MERGE (s:Stock {ticker: $ticker})
        SET s.company_name = $company_name,
            s.sector = $sector
        RETURN s
        """
        return self._execute_query(query, {
            "ticker": ticker,
            "company_name": company_name,
            "sector": sector
        })

    def record_price(self, ticker, date, open_price, close_price, high, low, volume):
        query = """
        MATCH (s:Stock {ticker: $ticker})
        MERGE (p:Price {stock_ticker: $ticker, date: $date})
        SET p.open = $open_price,
            p.close = $close_price,
            p.high = $high,
            p.low = $low,
            p.volume = $volume
        MERGE (s)-[r:HAS_PRICE]->(p)
        RETURN p
        """
        return self._execute_query(query, {
            "ticker": ticker,
            "date": date,
            "open_price": open_price,
            "close_price": close_price,
            "high": high,
            "low": low,
            "volume": volume
        })

    def record_transaction(self, user_id, ticker, action_type, amount, price, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now().isoformat()

        query = """
        MATCH (u:User {id: $user_id})
        MATCH (s:Stock {ticker: $ticker})
        CREATE (t:Transaction {
            type: $action_type,
            amount: $amount,
            price: $price,
            timestamp: $timestamp
        })
        CREATE (u)-[:MADE]->(t)
        CREATE (t)-[:RELATES_TO]->(s)
        RETURN t
        """
        return self._execute_query(query, {
            "user_id": user_id,
            "ticker": ticker,
            "action_type": action_type,
            "amount": amount,
            "price": price,
            "timestamp": timestamp
        })

    # --- Аналитические методы ---

    def get_portfolio(self, user_id):
        query = """
        MATCH (u:User {id: $user_id})-[:MADE]->(t:Transaction)-[:RELATES_TO]->(s:Stock)
        RETURN s.ticker as ticker, 
               sum(CASE WHEN t.type = 'BUY' THEN t.amount ELSE -t.amount END) as shares,
               s.company_name as company,
               s.sector as sector
        HAVING shares > 0
        """
        return self._execute_query(query, {"user_id": user_id})

    def get_price_history(self, ticker, days=30):
        query = """
        MATCH (s:Stock {ticker: $ticker})-[:HAS_PRICE]->(p:Price)
        WHERE p.date >= date().duration('-P%dD')
        RETURN p.date as date, p.close as price
        ORDER BY p.date
        """ % days
        return self._execute_query(query, {"ticker": ticker})

    def get_correlated_stocks(self, ticker, threshold=0.7):
        query = """
        MATCH (s1:Stock {ticker: $ticker})-[r:CORRELATES_WITH]->(s2:Stock)
        WHERE r.strength >= $threshold
        RETURN s2.ticker as ticker, r.strength as correlation
        ORDER BY r.strength DESC
        """
        return self._execute_query(query, {
            "ticker": ticker,
            "threshold": threshold
        })

    def calculate_portfolio_risk(self, user_id):
        query = """
        MATCH (u:User {id: $user_id})-[:MADE]->(t:Transaction)-[:RELATES_TO]->(s:Stock)
        WITH s, sum(CASE WHEN t.type = 'BUY' THEN t.amount ELSE -t.amount END) as shares
        WHERE shares > 0
        MATCH (s)-[r:CORRELATES_WITH]->(other:Stock)
        RETURN avg(r.volatility) as avg_volatility,
               count(DISTINCT s.sector) as sector_diversity
        """
        return self._execute_query(query, {"user_id": user_id})

    # --- Методы для работы с графом ---

    def export_graph(self, file_path="investment_graph.graphml"):
        query = """
        CALL apoc.export.graphml.all($file_path, {})
        YIELD file, nodes, relationships, properties
        RETURN file, nodes, relationships, properties
        """
        return self._execute_query(query, {"file_path": file_path})

    def clear_database(self):
        query = "MATCH (n) DETACH DELETE n"
        return self._execute_query(query)