-- wrk -t100 -c100 -d45 -s stress.lua --latency http://localhost:1024/?url=https%3A%2F%2Fwww.youtube.com%2Fwatch%3Fv%3DEnDXGQmCz3U%26t%3D0s

wrk.method = "POST"
wrk.headers["Authorization"] = "Bearer <token>"
wrk.headers["content-type"] = "application/json"
wrk.body = "{ \"name\": \"User1\", \"age\": 25, \"gender\": \"Female\", \"country\": \"India\"}"