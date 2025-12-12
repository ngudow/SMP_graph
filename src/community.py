from connection import Neo4jConnection

class CommunityDetection:
    GRAPH_NAME = "gds_investment_graph_comm"

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
          ['Stock'],
          {{
            CORRELATES_WITH: {{orientation: 'UNDIRECTED'}},
            AFFECTED_BY: {{orientation: 'UNDIRECTED'}}
          }}
        )
        """
        return self.conn.run(q)

    def louvain(self, write=False, write_property="louvain_community"):
        """
        Louvain community detection.
        Если write=True — пишет свойство на узлы.
        """
        self._project_graph()
        if write:
            q = f"""
            CALL gds.louvain.write('{self.GRAPH_NAME}', {{writeProperty: $write_property}})
            YIELD communityCount, ranIterations
            RETURN communityCount, ranIterations
            """
            return self.conn.run(q, {"write_property": write_property})
        else:
            q = f"""
            CALL gds.louvain.stream('{self.GRAPH_NAME}')
            YIELD nodeId, communityId
            RETURN gds.util.asNode(nodeId).ticker AS ticker,
                   communityId
            ORDER BY communityId
            """
            return self.conn.run(q)

    def label_propagation(self):
        """
        Label Propagation (stream).
        """
        self._project_graph()
        q = f"""
        CALL gds.labelPropagation.stream('{self.GRAPH_NAME}')
        YIELD nodeId, label
        RETURN gds.util.asNode(nodeId).ticker AS ticker,
               label
        ORDER BY label
        """
        return self.conn.run(q)

if __name__ == "__main__":
    import os, pprint
    conn = Neo4jConnection(os.getenv("NEO4J_URI","bolt://localhost:7687"),
                           os.getenv("NEO4J_USER","neo4j"),
                           os.getenv("NEO4J_PASS","password"))
    cd = CommunityDetection(conn)
    print("Louvain (stream):")
    pprint.pprint(cd.louvain())
    print("Label Propagation:")
    pprint.pprint(cd.label_propagation())
    conn.close()
