#!/bin/bash
# Distributed OptimusDB query tester with CSV logging (18001–18008)
# Logs all results in query_results_log.csv

PORTS=(18001 18002 18003 18004 18005 18006 18007 18008)
CONTENT_TYPE="application/json"
LOGFILE="query_results_log.csv"

# Initialize log header
echo "timestamp,query_file,optimusdb_port,status" > "$LOGFILE"

echo "=== Starting Distributed OptimusDB Query Tests ==="

i=0
for f in *.json; do
  PORT=${PORTS[$((i % ${#PORTS[@]}))]}
  TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")
  echo "➡️ Sending $f → OptimusDB:$PORT"

  if curl -s -X POST "http://localhost:${PORT}/swarmkb/command" \
    -H "Content-Type: $CONTENT_TYPE" \
    -d @"$f" \
    -o "response_${f%.json}_${PORT}.txt" \
    -w "%{http_code}" | grep -q "200"; then
      STATUS="Success"
  else
      STATUS="Fail"
  fi

  echo "${TIMESTAMP},${f},${PORT},${STATUS}" >> "$LOGFILE"
  echo "✅ ${f} logged as ${STATUS} at ${TIMESTAMP}"
  echo "---------------------------------------------"
  ((i++))
done

echo "=== All distributed queries completed ==="
echo "📄 Log written to: $LOGFILE"
