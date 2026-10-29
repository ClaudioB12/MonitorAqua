/*
  MonitorAqua - sensor real (ESP32-32D + JSN-SR04T/AJ-SR04M + DHT11)
  - Lee distancia (JSN-SR04T con placa AJ-SR04M) y humedad/temperatura (DHT11) cada 5 s.
  - Distancia = mediana de 5 mediciones (descarta ecos falsos); -1 si hay menos de 3 validas.
  - Corrige la velocidad del sonido con la temperatura del DHT11
    (la distanciaCm que llega a Firebase YA viene compensada).
  - Hora real por NTP (no publica hasta tener hora valida).
  - Escribe en Firebase Realtime Database (ultima lectura + historial).
  - Publica el mismo evento por MQTT (opcional, desactivado).
  - Reintenta WiFi, hora y MQTT hasta lograrlo, sin bloquear el envio a Firebase.

  Conexiones:
    AJ-SR04M VCC  -> VIN (5 V)      AJ-SR04M Trig -> GPIO 5
    AJ-SR04M Echo -> divisor 1k/2k -> GPIO 18      AJ-SR04M GND -> GND
      (Echo -> 1 kOhm -> GPIO 18 ; GPIO 18 -> 2 kOhm -> GND)
    DHT11 VCC     -> 3V3            DHT11 DATA    -> GPIO 4      DHT11 GND -> GND

  Placa AJ-SR04M: el punto R19 debe estar VACIO (modo Trig/Echo, igual que el HC-SR04).
  Zona ciega de la sonda: ~20-25 cm. Montarla al menos 25 cm sobre el nivel maximo del agua.

  Credenciales: en el archivo secrets.h (misma carpeta del sketch). No subirlo a GitHub.

  Bibliotecas (Arduino IDE): FirebaseClient (Mobizt), DHT sensor library (Adafruit),
  Adafruit Unified Sensor, PubSubClient (Nick O'Leary).
*/

#define ENABLE_USER_AUTH
#define ENABLE_DATABASE

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#define SSL_CLIENT WiFiClientSecure   // reemplaza a ExampleFunctions.h
#include <FirebaseClient.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <time.h>

#include "secrets.h"   // WIFI_SSID, WIFI_PASSWORD, Web_API_KEY, DATABASE_URL, USER_EMAIL, USER_PASS

// ---------------- MQTT (para el puente hacia Kafka) ----------------
#define USAR_MQTT    0          // 1 = publicar tambien por MQTT (la red debe permitir el puerto 1883)
#define MQTT_HOST    "test.mosquitto.org"
#define MQTT_PORT    1883
#define MQTT_TOPIC   "lambda26/aqua/horeb/real"
#define SENSOR_ID    "esp32-tanque-real-01"

// ---------------- Pines ----------------
#define PIN_TRIG  5
#define PIN_ECHO  18
#define PIN_DHT   4
#define DHTTYPE   DHT11          // cambia a DHT22 si ese es tu sensor

// ---------------- Medicion ultrasonica (JSN-SR04T / AJ-SR04M) ----------------
#define N_MEDICIONES        5        // mediciones por lectura (se usa la mediana)
#define MIN_VALIDAS         3        // minimo de mediciones validas para aceptar la lectura
#define PAUSA_ENTRE_MS      70       // el sensor necesita ~60 ms entre disparos
#define PULSO_TRIG_US       20       // la AJ-SR04M pide un pulso de disparo de ~20 us
#define TIMEOUT_ECO_US      30000    // ~5 m maximo

const unsigned long INTERVALO_MS = 5000;
const time_t TS_MIN_S = 1767225600;   // 01/01/2026 UTC: antes de esto la hora no es valida

// ---------------- Objetos ----------------
DHT dht(PIN_DHT, DHTTYPE);

void processData(AsyncResult &aResult);
UserAuth user_auth(Web_API_KEY, USER_EMAIL, USER_PASS);
SSL_CLIENT ssl_client;        // autenticacion + /lecturas/ultima
SSL_CLIENT ssl_client_hist;   // /lecturas/historial (conexion propia, no compite con la otra)
FirebaseApp app;
using AsyncClient = AsyncClientClass;
AsyncClient aClient(ssl_client);
AsyncClient aClientHist(ssl_client_hist);
RealtimeDatabase Database;

WiFiClient netMqtt;
PubSubClient mqtt(netMqtt);

unsigned long ultimoEnvio = 0;
unsigned long ultimoIntentoWifi = 0;
unsigned long ultimoIntentoHora = 0;
unsigned long ultimoIntentoMqtt = 0;

// ---------------- Utilidades ----------------
const char* textoEstadoWifi(wl_status_t st) {
  switch (st) {
    case WL_NO_SSID_AVAIL:  return "no encuentra la red (nombre incorrecto, apagada o en 5 GHz)";
    case WL_CONNECT_FAILED: return "fallo la conexion (revisa la contrasena)";
    case WL_CONNECTION_LOST:return "conexion perdida";
    case WL_DISCONNECTED:   return "desconectado";
    case WL_IDLE_STATUS:    return "esperando";
    default:                return "otro estado";
  }
}

void listarRedes() {
  WiFi.disconnect();          // detener el intento de conexion para poder escanear
  delay(100);
  Serial.println("Buscando redes...");
  int n = WiFi.scanNetworks();
  if (n <= 0) Serial.println("  (ninguna)");
  for (int i = 0; i < n; i++)
    Serial.printf("  %s  (%d dBm, canal %d)\n", WiFi.SSID(i).c_str(), WiFi.RSSI(i), WiFi.channel(i));
  WiFi.scanDelete();
}

// Bloquea hasta conectarse: reintenta indefinidamente, cada 15 s.
void initWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);            // conexion mas estable con zonas Wi-Fi de celular
  WiFi.setAutoReconnect(true);
  int intento = 0;
  while (WiFi.status() != WL_CONNECTED) {
    intento++;
    Serial.printf("\nConectando a \"%s\" (intento %d)", WIFI_SSID, intento);
    WiFi.disconnect();
    delay(200);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    unsigned long t0 = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - t0 < 15000) {
      Serial.print('.');
      delay(500);
    }
    if (WiFi.status() != WL_CONNECTED) {
      Serial.printf("\nSin conexion: %s\n", textoEstadoWifi(WiFi.status()));
      if (intento % 4 == 1) listarRedes();   // al primer fallo y luego cada 4 intentos
    }
  }
  Serial.print("\nWiFi conectado. IP: ");
  Serial.println(WiFi.localIP());
}

// En el loop: si se cae el WiFi, reintenta cada 10 s sin bloquear.
bool wifiListo() {
  if (WiFi.status() == WL_CONNECTED) return true;
  if (millis() - ultimoIntentoWifi > 10000) {
    ultimoIntentoWifi = millis();
    Serial.printf("WiFi perdido (%s), reconectando...\n", textoEstadoWifi(WiFi.status()));
    WiFi.disconnect();
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  }
  return false;
}

void initHora() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.print("Sincronizando hora");
  unsigned long t0 = millis();
  while (time(nullptr) < TS_MIN_S && millis() - t0 < 20000) { Serial.print('.'); delay(500); }
  Serial.println(time(nullptr) >= TS_MIN_S ? " OK" : " sin hora (se reintentara)");
}

bool horaValida() { return time(nullptr) >= TS_MIN_S; }

uint64_t epochMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

// Una medicion; usa la temperatura para la velocidad del sonido. -1 si no hay eco.
float medirUnaVez(float tempC) {
  digitalWrite(PIN_TRIG, LOW);  delayMicroseconds(5);
  digitalWrite(PIN_TRIG, HIGH); delayMicroseconds(PULSO_TRIG_US);
  digitalWrite(PIN_TRIG, LOW);
  unsigned long t = pulseIn(PIN_ECHO, HIGH, TIMEOUT_ECO_US);
  if (t == 0) return -1;
  float vSonido = 331.3 + 0.606 * (isnan(tempC) ? 20.0 : tempC);   // m/s
  return t * (vSonido / 10000.0) / 2.0;                            // cm
}

// Mediana de N_MEDICIONES validas para descartar ecos falsos. -1 si hay menos de MIN_VALIDAS.
float leerDistanciaCm(float tempC) {
  float v[N_MEDICIONES];
  int n = 0;
  for (int i = 0; i < N_MEDICIONES; i++) {
    float d = medirUnaVez(tempC);
    if (d > 0) v[n++] = d;
    delay(PAUSA_ENTRE_MS);
  }
  if (n < MIN_VALIDAS) return -1;
  for (int i = 1; i < n; i++) {                 // ordenar (insercion)
    float k = v[i];
    int j = i - 1;
    while (j >= 0 && v[j] > k) { v[j + 1] = v[j]; j--; }
    v[j + 1] = k;
  }
  return v[n / 2];
}

void conectarMqtt() {
#if USAR_MQTT
  if (mqtt.connected()) return;
  ultimoIntentoMqtt = millis();
  String clientId = String(SENSOR_ID) + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  if (mqtt.connect(clientId.c_str())) Serial.println("MQTT conectado");
  else { Serial.print("MQTT fallo, rc="); Serial.println(mqtt.state()); }
#endif
}

// ---------------- Setup / loop ----------------
void setup() {
  Serial.begin(115200);
  pinMode(PIN_TRIG, OUTPUT);
  digitalWrite(PIN_TRIG, LOW);
  pinMode(PIN_ECHO, INPUT);
  dht.begin();

  initWiFi();
  initHora();

  // Tiempos holgados: con WiFi lento (zona de celular) 1 s no alcanza y las tareas se cancelan (-118)
  ssl_client.setInsecure();
  ssl_client.setConnectionTimeout(5000);   // ms
  ssl_client.setHandshakeTimeout(15);      // s
  ssl_client_hist.setInsecure();
  ssl_client_hist.setConnectionTimeout(5000);
  ssl_client_hist.setHandshakeTimeout(15);

  initializeApp(aClient, app, getAuth(user_auth), processData, "authTask");
  app.getApp<RealtimeDatabase>(Database);
  Database.url(DATABASE_URL);

#if USAR_MQTT
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  conectarMqtt();
#endif
}

void loop() {
  if (!wifiListo()) { delay(100); return; }   // sin WiFi no se hace nada mas
  app.loop();

  static unsigned long ultimoRssi = 0;        // senal WiFi cada 60 s (diagnostico)
  if (millis() - ultimoRssi > 60000) {
    ultimoRssi = millis();
    Serial.printf("WiFi RSSI: %d dBm\n", WiFi.RSSI());
  }
#if USAR_MQTT
  if (!mqtt.connected() && millis() - ultimoIntentoMqtt > 15000) conectarMqtt();
  mqtt.loop();
#endif

  if (millis() - ultimoEnvio < INTERVALO_MS) return;
  ultimoEnvio = millis();

  if (!horaValida()) {
    Serial.println("Sin hora valida: no se publica");
    if (millis() - ultimoIntentoHora > 30000) {
      ultimoIntentoHora = millis();
      configTime(0, 0, "pool.ntp.org", "time.google.com");
    }
    return;
  }

  float hum  = dht.readHumidity();
  float temp = dht.readTemperature();
  float dist = leerDistanciaCm(temp);   // tarda ~0.35 s (5 mediciones)
  app.loop();                           // atender a Firebase despues de medir
  uint64_t ts = epochMs();

  // Evento JSON (los valores invalidos se envian como null para que el consumer los marque)
  char sHum[12], sTemp[12], sDist[12];
  if (isnan(hum))  strcpy(sHum, "null");  else dtostrf(hum, 1, 1, sHum);
  if (isnan(temp)) strcpy(sTemp, "null"); else dtostrf(temp, 1, 1, sTemp);
  if (dist < 0)    strcpy(sDist, "null"); else dtostrf(dist, 1, 1, sDist);

  char payload[320];
  snprintf(payload, sizeof(payload),
    "{\"tipoEvento\":\"aqua.tanque.lectura\",\"sensorId\":\"%s\",\"distanciaCm\":%s,"
    "\"humedadPct\":%s,\"temperaturaC\":%s,\"origen\":\"esp32-fisico\",\"timestamp\":%llu}",
    SENSOR_ID, sDist, sHum, sTemp, (unsigned long long)ts);
  Serial.println(payload);

#if USAR_MQTT
  if (mqtt.connected()) mqtt.publish(MQTT_TOPIC, payload);
#endif

  // Firebase: ultima lectura (se sobrescribe) + historial (se agrega)
  if (app.ready()) {
    object_t json, o1, o2, o3, o4, o5;
    JsonWriter writer;
    writer.create(o1, "distanciaCm", number_t(dist < 0 ? -1 : dist, 1));
    writer.create(o2, "humedadPct", number_t(isnan(hum) ? -1 : hum, 1));
    writer.create(o3, "temperaturaC", number_t(isnan(temp) ? -1 : temp, 1));
    writer.create(o4, "sensorId", string_t(SENSOR_ID));
    writer.create(o5, "timestamp", number_t((double)ts, 0));
    writer.join(json, 5, o1, o2, o3, o4, o5);

    Database.set<object_t>(aClient, "/board1/lecturas/ultima", json, processData, "setUltima");
    Database.push<object_t>(aClientHist, "/board1/lecturas/historial", json, processData, "pushHistorial");
  }
}

void processData(AsyncResult &aResult) {
  if (!aResult.isResult()) return;
  if (aResult.isEvent())
    Firebase.printf("Evento %s: %s (%d)\n", aResult.uid().c_str(),
                    aResult.eventLog().message().c_str(), aResult.eventLog().code());
  if (aResult.isError())
    Firebase.printf("Error %s: %s (%d)\n", aResult.uid().c_str(),
                    aResult.error().message().c_str(), aResult.error().code());
  if (aResult.available())
    Firebase.printf("OK %s\n", aResult.uid().c_str());
}
