/*
  MonitorAqua - Nodo maestro (firmware completo)
  ESP32 Dev Module · Monitor Serie a 115200

  Sensores (pines ya probados con el diagnóstico):
    AJ-SR04M   Trig -> GPIO 18 · Echo -> divisor 3x1k -> GPIO 19 · VCC 5 V
    DHT11      DATA -> GPIO 4 · VCC 3,3 V
    YF-S201    entrada -> divisor -> GPIO 25 · salida -> divisor -> GPIO 26
    ADS1115    SDA 21 · SCL 22 · A0 = NODO del potenciómetro (presión simulada)

  Escribe en Firebase RTDB:
    /board1/lecturas/ultima      (se sobrescribe cada 5 s)
    /board1/lecturas/historial   (se agrega cada HISTORIAL_MS)
  Valores inválidos = -1 (mismo contrato que antes).

  Librerías: FirebaseClient (Mobizt), Adafruit ADS1X15, DHT sensor library,
  Adafruit Unified Sensor. Deja ExampleFunctions.h en la carpeta del sketch.
*/

#define ENABLE_USER_AUTH
#define ENABLE_DATABASE

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <FirebaseClient.h>
#include "ExampleFunctions.h"
#include <Wire.h>
#include <Adafruit_ADS1X15.h>
#include <DHT.h>
#include <time.h>
#include "secrets.h"   // WIFI_SSID, WIFI_PASSWORD, Web_API_KEY, DATABASE_URL, USER_EMAIL, USER_PASS

// ---------------- Credenciales (no subir a GitHub) ----------------


#define SENSOR_ID "esp32-tanque-real-01"

// ---------------- Pines ----------------
const int PIN_TRIG = 18;
const int PIN_ECHO = 19;
const int PIN_DHT = 4;
const int PIN_FLUJO_IN = 25;
const int PIN_FLUJO_OUT = 26;
#define DHTTYPE DHT11

// ---------------- Parámetros ----------------
const float FACTOR_DIVISOR = 1.5;        // 3 de 10 kΩ en el divisor del potenciómetro
const float HZ_POR_LMIN = 7.5;           // YF-S201: f(Hz) = 7,5 x Q(L/min)
const float PULSOS_POR_LITRO = 450.0;    // YF-S201
const unsigned long INTERVALO_MS = 5000; // lectura y envío de "ultima"
const unsigned long HISTORIAL_MS = 5000; // súbelo a 60000 para que el historial crezca menos
const time_t TS_MIN_S = 1767225600;      // 01/01/2026: antes de esto la hora no es válida

// ---------------- Objetos ----------------
DHT dht(PIN_DHT, DHTTYPE);
Adafruit_ADS1115 ads;
bool hayADS = false;

void processData(AsyncResult &aResult);
UserAuth user_auth(Web_API_KEY, USER_EMAIL, USER_PASS);
SSL_CLIENT ssl_client;
FirebaseApp app;
using AsyncClient = AsyncClientClass;
AsyncClient aClient(ssl_client);
RealtimeDatabase Database;

volatile uint32_t pulsosIn = 0, pulsosOut = 0;
void IRAM_ATTR cuentaIn()  { pulsosIn++; }
void IRAM_ATTR cuentaOut() { pulsosOut++; }

double litrosIn = 0, litrosOut = 0;
unsigned long ultimoEnvio = 0, ultimoHist = 0, ultimoWifi = 0;

// ---------------- WiFi y hora ----------------
void initWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.print("Conectando a WiFi");
  unsigned long t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 20000) { Serial.print('.'); delay(500); }
  if (WiFi.status() == WL_CONNECTED) { Serial.print("\nIP: "); Serial.println(WiFi.localIP()); }
  else Serial.println("\nSin WiFi (se reintentará). ¿La red es de 2,4 GHz?");
}

void revisarWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  if (millis() - ultimoWifi < 10000) return;
  ultimoWifi = millis();
  Serial.println("WiFi caído: reconectando...");
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

void initHora() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  Serial.print("Sincronizando hora");
  unsigned long t0 = millis();
  while (time(nullptr) < TS_MIN_S && millis() - t0 < 20000) { Serial.print('.'); delay(500); }
  Serial.println(time(nullptr) >= TS_MIN_S ? " OK" : " sin hora (se reintentará)");
}

bool horaValida() { return time(nullptr) >= TS_MIN_S; }

uint64_t epochMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

// ---------------- Sensores ----------------
// Distancia en cm corregida con la temperatura. -1 si no hay eco o está fuera de 20-600 cm.
float leerDistanciaCm(float tempC) {
  digitalWrite(PIN_TRIG, LOW);  delayMicroseconds(5);
  digitalWrite(PIN_TRIG, HIGH); delayMicroseconds(20);   // AJ-SR04M: >= 10 us
  digitalWrite(PIN_TRIG, LOW);
  unsigned long t = pulseIn(PIN_ECHO, HIGH, 50000);
  if (t == 0) return -1;
  float vSonido = 331.3 + 0.606 * (tempC < -40 ? 20.0 : tempC);  // m/s
  float d = t * (vSonido / 10000.0) / 2.0;
  if (d < 20 || d > 600) return -1;   // zona ciega o sin eco (el sensor da ~807 cm)
  return d;
}

float leerNodoV() {
  if (!hayADS) return -1;
  long suma = 0;
  for (int i = 0; i < 10; i++) suma += ads.readADC_SingleEnded(0);
  return ads.computeVolts((int16_t)(suma / 10));
}

const char* estadoPresion(float p) {
  if (p < 0)      return "sin_senal";
  if (p < 0.02)   return "corte_servicio";
  if (p < 0.15)   return "presion_baja";
  if (p <= 0.35)  return "servicio_normal";
  if (p < 0.45)   return "presion_alta";
  return "fondo_escala";
}

// ---------------- Setup ----------------
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n=== MonitorAqua · nodo maestro ===");

  pinMode(PIN_TRIG, OUTPUT);
  pinMode(PIN_ECHO, INPUT);
  pinMode(PIN_FLUJO_IN, INPUT);
  pinMode(PIN_FLUJO_OUT, INPUT);
  attachInterrupt(digitalPinToInterrupt(PIN_FLUJO_IN), cuentaIn, FALLING);
  attachInterrupt(digitalPinToInterrupt(PIN_FLUJO_OUT), cuentaOut, FALLING);
  dht.begin();

  Wire.begin(21, 22);
  hayADS = ads.begin(0x48);
  if (hayADS) { ads.setGain(GAIN_ONE); Serial.println("ADS1115 OK"); }
  else Serial.println("ADS1115 no encontrado: la presión se enviará como -1");

  initWiFi();
  initHora();

  ssl_client.setInsecure();
  ssl_client.setConnectionTimeout(1000);
  ssl_client.setHandshakeTimeout(5);

  initializeApp(aClient, app, getAuth(user_auth), processData, "authTask");
  app.getApp<RealtimeDatabase>(Database);
  Database.url(DATABASE_URL);

  ultimoEnvio = millis();
}

// ---------------- Loop ----------------
void loop() {
  app.loop();
  revisarWiFi();

  unsigned long ahora = millis();
  if (ahora - ultimoEnvio < INTERVALO_MS) return;
  float dt = (ahora - ultimoEnvio) / 1000.0;
  ultimoEnvio = ahora;

  // Caudal: pulsos de este intervalo
  noInterrupts();
  uint32_t pIn = pulsosIn, pOut = pulsosOut;
  pulsosIn = 0; pulsosOut = 0;
  interrupts();
  float qIn  = (pIn / dt) / HZ_POR_LMIN;
  float qOut = (pOut / dt) / HZ_POR_LMIN;
  litrosIn  += pIn / PULSOS_POR_LITRO;
  litrosOut += pOut / PULSOS_POR_LITRO;

  // DHT11
  float hum = dht.readHumidity();
  float temp = dht.readTemperature();
  if (isnan(hum))  hum = -1;
  if (isnan(temp)) temp = -100;   // marca interna; se envía como -1

  // Distancia (corregida con la temperatura)
  float dist = leerDistanciaCm(temp);
  if (temp == -100) temp = -1;

  // Presión simulada (potenciómetro)
  float vNodo = leerNodoV();
  float pMPa = -1;
  if (vNodo >= 0.05) {
    pMPa = (vNodo * FACTOR_DIVISOR - 0.5) * 0.125;
    if (pMPa < 0) pMPa = 0;
  }
  const char* estado = estadoPresion(pMPa);

  Serial.printf("Dist %.1f cm | T %.1f C | H %.0f %% | P %.3f MPa (%s, NODO %.2f V) | "
                "Qin %.2f L/min | Qout %.2f L/min | Vin %.2f L | Vout %.2f L\n",
                dist, temp, hum, pMPa, estado, vNodo, qIn, qOut, litrosIn, litrosOut);

  if (!horaValida()) { Serial.println("Sin hora válida: no se envía"); initHora(); return; }
  if (!app.ready())  { Serial.println("Firebase aún no listo (autenticando)"); return; }

  uint64_t ts = epochMs();

  object_t json, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13;
  JsonWriter writer;
  writer.create(o1,  "distanciaCm",       number_t(dist, 1));
  writer.create(o2,  "humedadPct",        number_t(hum, 1));
  writer.create(o3,  "temperaturaC",      number_t(temp, 1));
  writer.create(o4,  "presionMPa",        number_t(pMPa, 3));
  writer.create(o5,  "nodoPresionV",      number_t(vNodo, 3));
  writer.create(o6,  "estadoPresion",     string_t(estado));
  writer.create(o7,  "caudalEntradaLmin", number_t(qIn, 2));
  writer.create(o8,  "caudalSalidaLmin",  number_t(qOut, 2));
  writer.create(o9,  "volumenEntradaL",   number_t(litrosIn, 3));
  writer.create(o10, "volumenSalidaL",    number_t(litrosOut, 3));
  writer.create(o11, "sensorId",          string_t(SENSOR_ID));
  writer.create(o12, "origenPresion",     string_t("potenciometro_simulado"));
  writer.create(o13, "timestamp",         number_t((double)ts, 0));
  writer.join(json, 13, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13);

  Database.set<object_t>(aClient, "/board1/lecturas/ultima", json, processData, "setUltima");

  if (ahora - ultimoHist >= HISTORIAL_MS) {
    ultimoHist = ahora;
    Database.push<object_t>(aClient, "/board1/lecturas/historial", json, processData, "pushHistorial");
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