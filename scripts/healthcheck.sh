#!/bin/bash
# ─────────────────────────────────────────────
#  System health check
#  Verifies all services are up and responding
# ─────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
  local NAME=$1
  local CMD=$2
  if eval "$CMD" &>/dev/null; then
    echo -e "  ${GREEN}✓${NC} $NAME"
    ((PASS++))
  else
    echo -e "  ${RED}✗${NC} $NAME"
    ((FAIL++))
  fi
}

echo ""
echo "═══════════════════════════════════════"
echo "  Trading System — Health Check"
echo "═══════════════════════════════════════"
echo ""
echo "Docker containers:"
check "Postgres"          "docker exec trading_postgres pg_isready -U trading"
check "Redis"             "docker exec trading_redis redis-cli ping"
check "Redpanda"          "docker exec trading_redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'"
check "MinIO"             "curl -sf http://localhost:9000/minio/health/live"
check "Prometheus"        "curl -sf http://localhost:9090/-/healthy"
check "Grafana"           "curl -sf http://localhost:3000/api/health"
check "Redpanda Console"  "curl -sf http://localhost:8080/api/cluster/overview"

echo ""
echo "Postgres extensions:"
check "pgvector"          "docker exec trading_postgres psql -U trading -d trading_db -c \"SELECT * FROM pg_extension WHERE extname='vector'\" | grep -q vector"
check "uuid-ossp"         "docker exec trading_postgres psql -U trading -d trading_db -c \"SELECT * FROM pg_extension WHERE extname='uuid-ossp'\" | grep -q uuid"
check "pg_trgm"           "docker exec trading_postgres psql -U trading -d trading_db -c \"SELECT * FROM pg_extension WHERE extname='pg_trgm'\" | grep -q pg_trgm"

echo ""
echo "Kafka topics:"
check "news.raw"          "docker exec trading_redpanda rpk topic list 2>/dev/null | grep -q 'news.raw'"
check "prices.bars"       "docker exec trading_redpanda rpk topic list 2>/dev/null | grep -q 'prices.bars'"
check "events.calendar"   "docker exec trading_redpanda rpk topic list 2>/dev/null | grep -q 'events.calendar'"

echo ""
echo "MinIO buckets:"
check "news-fulltext"     "docker exec trading_minio mc ls local/news-fulltext 2>/dev/null || curl -sf http://localhost:9000/news-fulltext"

echo ""
echo "───────────────────────────────────────"
echo -e "  ${GREEN}Passed: $PASS${NC}  ${RED}Failed: $FAIL${NC}"
echo "───────────────────────────────────────"

if [ $FAIL -gt 0 ]; then
  echo ""
  echo -e "${YELLOW}Some checks failed. Run: docker-compose logs <service>${NC}"
  exit 1
fi

echo ""
echo -e "${GREEN}All systems operational ✓${NC}"
echo ""
echo "Access points:"
echo "  Grafana:          http://localhost:3000  (admin/admin123)"
echo "  Redpanda Console: http://localhost:8080"
echo "  MinIO Console:    http://localhost:9001  (minioadmin/minioadmin123)"
echo "  Prometheus:       http://localhost:9090"
echo "  Adminer (DB):     http://localhost:8080"
