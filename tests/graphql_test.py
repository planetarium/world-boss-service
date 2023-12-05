def test_next_tx_nonce(fx_session, fx_test_client):
    query = "query { nextTxNonce }"
    resp = fx_test_client.post("/graphql", json={"query": query})
    result = resp.json()
    assert result["data"]["nextTxNonce"] == 1
