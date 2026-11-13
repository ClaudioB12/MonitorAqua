@echo off
REM U3 (hardening): el Prometheus de lambda26/obs espera un Pushgateway en pushgateway:9091 (job "pushgateway"),
REM pero en esta máquina nunca se creó (target DOWN). Mismo servicio que lambda26/obs/compose.yml, con healthcheck.
REM Si ya existe y está detenido:  docker start lambda26-pushgateway
docker run -d --name lambda26-pushgateway --restart unless-stopped ^
  --network lambda26-kafka-net --network-alias pushgateway -p 49091:9091 ^
  --health-cmd "wget -q --spider http://localhost:9091/-/ready" --health-interval 10s --health-timeout 5s ^
  --health-retries 10 --health-start-period 10s ^
  prom/pushgateway:v1.11.1
