from connection import Neo4jConnection

class Centrality:
    GRAPH_NAME = "gds_investment_graph_centrality"

    def __init__(self, neo4j_conn: Neo4jConnection):
        self.conn = neo4j_conn

    def _project_graph(self):
        try:
            self.conn.run(f"CALL gds.graph.drop('{self.GRAPH_NAME}') YIELD graphName")
        except Exception:
            pass

        q = f"""
        CALL gds.graph.project(
          '{self.GRAPH_NAME}',
          ['Stock','Factor'],
          ['CORRELATES_WITH','AFFECTED_BY']
        )
        """
        return self.conn.run(q)

    def betweenness(self, top_n=50):
        """
        Betweenness centrality (stream).
        Возвращает список {ticker, score}
        """
        self._project_graph()
        q = f"""
        CALL gds.betweenness.stream('{self.GRAPH_NAME}')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).ticker AS ticker,
               labels(gds.util.asNode(nodeId)) AS labels,
               score
        ORDER BY score DESC
        LIMIT $top_n
        """
        return self.conn.run(q, {"top_n": top_n})

    def closeness(self, top_n=50):
        """
        Closeness centrality (stream).
        """
        self._project_graph()
        q = f"""
        CALL gds.alpha.closeness.stream('{self.GRAPH_NAME}')
        YIELD nodeId, centrality
        RETURN gds.util.asNode(nodeId).ticker AS ticker,
               labels(gds.util.asNode(nodeId)) AS labels,
               centrality
        ORDER BY centrality DESC
        LIMIT $top_n
        """
        try:
            return self.conn.run(q, {"top_n": top_n})
        except Exception:
            q2 = q.replace("gds.alpha.closeness", "gds.closeness")
            return self.conn.run(q2, {"top_n": top_n})

if __name__ == "__main__":
    import os, pprint
    conn = Neo4jConnection(os.getenv("NEO4J_URI","bolt://localhost:7687"),
                           os.getenv("NEO4J_USER","neo4j"),
                           os.getenv("NEO4J_PASS","password"))
    c = Centrality(conn)
    print("Betweenness:")
    pprint.pprint(c.betweenness(20))
    print("Closeness:")
    pprint.pprint(c.closeness(20))
    conn.close()
