from connection import Neo4jConnection

class MultiHop:
    def __init__(self, neo4j_conn: Neo4jConnection):
        self.conn = neo4j_conn

    def multi_hop_neighbors(self, ticker, max_hops=3, rels=None, limit=100):
        
        rel_filter = ''
        if rels:
            rel_filter = ':' + '|'.join(rels)

        query = f"""
        MATCH (start:Stock {{ticker: $ticker}})
        CALL apoc.path.expandConfig(start, {{
            relationshipFilter: "{'|'.join([f'{r}>' for r in rels]) if rels else '>'}",
            maxLevel: $max_hops,
            minLevel: 1,
            limit: $limit,
            labelFilter: "+Stock|+Factor|+User"
        }}) YIELD path
        WITH path, last(nodes(path)) AS lastNode, length(path) AS distance
        WHERE lastNode:Stock
        RETURN DISTINCT lastNode.ticker AS ticker,
               labels(lastNode) AS labels,
               distance
        ORDER BY distance
        LIMIT $limit
        """
        try:
            res = self.conn.run(query, {"ticker": ticker, "max_hops": max_hops, "limit": limit})
            return res
        except Exception:
            fallback_q = """
            MATCH (s1:Stock {ticker: $ticker})-[*1..$max_hops]-(s2:Stock)
            RETURN DISTINCT s2.ticker AS ticker, labels(s2) AS labels, length(shortestPath((s1)-[*1..$max_hops]-(s2))) AS distance
            ORDER BY distance
            LIMIT $limit
            """
            return self.conn.run(fallback_q, {"ticker": ticker, "max_hops": max_hops, "limit": limit})

if __name__ == "__main__":
    import os
    import pprint
    conn = Neo4jConnection(os.getenv("NEO4J_URI","bolt://localhost:7687"),
                           os.getenv("NEO4J_USER","neo4j"),
                           os.getenv("NEO4J_PASS","password"))
    mh = MultiHop(conn)
    pprint.pprint(mh.multi_hop_neighbors("AAPL", max_hops=3, rels=["AFFECTED_BY","CORRELATES_WITH"], limit=50))
    conn.close()
