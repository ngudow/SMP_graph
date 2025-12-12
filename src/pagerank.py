from connection import Neo4jConnection

class PageRank:
    """
    PageRank using Neo4j GDS. Создаёт проекцию графа 'gds_investment_graph' (если не существует),
    запускает PageRank и возвращает результаты.
    """
    GRAPH_NAME = "gds_investment_graph"

    def __init__(self, neo4j_conn: Neo4jConnection):
        self.conn = neo4j_conn

    def _ensure_graph(self):
        drop_q = f"CALL gds.graph.exists('{self.GRAPH_NAME}') YIELD exists " \
                 f"CALL apoc.do.when(exists, 'CALL gds.graph.drop(\"{self.GRAPH_NAME}\") YIELD graphName RETURN graphName', 'RETURN NULL as graphName', {{}}) YIELD value RETURN value"
        try:
            self.conn.run(drop_q)
        except Exception:
            pass

        project_q = f"""
        CALL gds.graph.project(
          '{self.GRAPH_NAME}',
          ['Stock','Factor'],
          {{
            HAS_PRICE: {{orientation: 'UNDIRECTED'}},
            CORRELATES_WITH: {{orientation: 'UNDIRECTED'}},
            AFFECTED_BY: {{orientation: 'UNDIRECTED'}},
            MADE: {{orientation: 'UNDIRECTED'}},
            RELATES_TO: {{orientation: 'UNDIRECTED'}}
          }}
        )
        """
        return self.conn.run(project_q)

    def run_pagerank(self, max_iter=20, damping=0.85, write=False, write_property="pagerank"):
        """
        Запустить PageRank. Если write=True — записать значение в узлы Stock:node[write_property]=score
        Возвращает список {nodeId, nodeProperties..., score}
        """
        self._ensure_graph()

        if write:
            q = f"""
            CALL gds.pageRank.write('{self.GRAPH_NAME}', {{maxIterations: $max_iter, dampingFactor: $damping, writeProperty: $write_property}})
            YIELD nodePropertiesWritten, ranIterations
            RETURN nodePropertiesWritten, ranIterations
            """
            return self.conn.run(q, {"max_iter": max_iter, "damping": damping, "write_property": write_property})
        else:
            q = f"""
            CALL gds.pageRank.stream('{self.GRAPH_NAME}', {{maxIterations: $max_iter, dampingFactor: $damping}})
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).ticker AS ticker,
                   labels(gds.util.asNode(nodeId)) AS labels,
                   score
            ORDER BY score DESC
            """
            return self.conn.run(q, {"max_iter": max_iter, "damping": damping})

if __name__ == "__main__":
    import os, pprint
    conn = Neo4jConnection(os.getenv("NEO4J_URI","bolt://localhost:7687"),
                           os.getenv("NEO4J_USER","neo4j"),
                           os.getenv("NEO4J_PASS","password"))
    pr = PageRank(conn)
    pprint.pprint(pr.run_pagerank())
    conn.close()
