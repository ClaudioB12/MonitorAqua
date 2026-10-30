# Firmware del ESP32 (`esp32-tanque-real-01`)

`MonitorAqua_esp32.ino` es la versión que corre en la placa. Las credenciales (Wi-Fi y Firebase) van en `secrets.h`,
que **nunca** se sube a git (`.gitignore`). La placa corre una versión de un solo archivo, con las credenciales
adentro y la misma lógica.

| Periodo | Sensor de distancia | Firmware | Rango válido |
|---|---|---|---|
| hasta 2026-10-29 09:22:23 (Lima) | HC-SR04 | misma lógica, pulso de disparo de 10 µs y **una** medición por lectura (no versionado) | 2-400 cm |
| desde 2026-10-29 09:22:23 | JSN-SR04T con placa AJ-SR04M | este `.ino`: pulso de 20 µs, mediana de 5 mediciones (-1 si hay menos de 3 válidas) | 25-450 cm (zona ciega ~20-25 cm) |

En las dos versiones:

- **Compensación térmica:** el firmware convierte el tiempo de vuelo con `vSonido = 331.3 + 0.606 · T` del DHT11, o
  con 20 °C si el DHT11 falla. La `distanciaCm` que llega a Firebase ya viene compensada. Por eso
  `ingesta/tanques.yml` → `compensacion_en_firmware` evita que el detector la compense otra vez.
- **Envío:** una lectura cada 5 s a `/board1/lecturas/historial`, con hora NTP; los valores inválidos llegan como -1.

Los periodos están en `ingesta/tanques.yml` → `sensor_desde` y `sensorModelo`. El cambio de sensor se instaló entre
las 08:32 y las 09:22 del 29-oct; `sensor_desde` es la primera lectura del salto de distancia.
