# Brief del proyecto

## 1. Datos del equipo

| Campo | Valor |
|---|---|
| Nombre del equipo | Horeb |
| Sección | [COMPLETAR] |
| URL del repositorio | <https://github.com/ClaudioB12/MonitorAqua> |
| Topics configurados (sí/no) | [COMPLETAR] |

### Integrantes y énfasis de coordinación

| Integrante | Énfasis de coordinación |
|---|---|
| Integrante A | Batch / Spark |
| Integrante B | Streaming / Kafka |
| Integrante C | BI / ML |
| Integrante D | Observabilidad e instrumentación |

!!! note "Aclaración de la plantilla"
    El rol es un **énfasis de coordinación**, no una división del trabajo técnico: cada integrante construye de extremo a extremo sus dos dimensiones (una U1 batch y una U2 streaming).

## 2. Dominio del proyecto

**Problema.** En Juliaca el suministro de agua potable es intermitente. Los hogares dependen de tanques de almacenamiento y conviven con tres incertidumbres: cuánta agua les queda y cuánto les durará, cuándo volverá a llegar el agua a su zona, y cuánta agua se pierde por fugas invisibles (un flotador vencido o una tubería con pérdida lenta que nadie nota hasta que sube el recibo). A esto se suma la calidad del agua ya almacenada: en el altiplano, las noches de helada y los tiempos largos de residencia comprometen la temperatura y el cloro residual.

**Dominio de datos.** Saneamiento / IoT domiciliario.

**Pregunta central.**

> **¿Cómo anticipar la disponibilidad y la calidad del agua en los hogares de Juliaca que dependen de tanques de almacenamiento a causa del suministro intermitente?**

La pregunta contiene dos variables de interés: **disponibilidad** (cuánta agua hay, cuándo llega, cuánta se pierde) y **calidad** (en qué estado está el agua que ya está almacenada). Cada integrante toma un aspecto de una de esas dos variables y lo trabaja en sus dos dimensiones, U1 y U2.

**Usuarios.**

- Las **familias** de Juliaca que dependen de tanques domiciliarios.
- **Seda Juliaca** (EPS), para contrastar continuidad declarada contra observada y focalizar pérdidas.
- **SUNASS — ODS Puno**, como regulador del servicio.
- La **municipalidad**, para priorización de intervenciones por zona.

**Arquitectura elegida: Lambda.** El proyecto necesita a la vez respuestas en **tiempo real** (autonomía proyectada, próxima llegada, fuga activa, riesgo térmico) y análisis **histórico** con modelos entrenados sobre semanas de acumulación y series públicas de años. La arquitectura Lambda cubre ambos frentes con una capa speed y una capa batch que comparten la misma ingesta. Se **descarta Kappa** porque el análisis histórico exigiría reprocesar todo desde Kafka, y Kafka no es un almacén de largo plazo.

**Fuente batch (real).**

- **SUNASS — Benchmarking Regulatorio** para Seda Juliaca: continuidad (h/día), agua no facturada (%), micromedición (%), presión, densidad de roturas — series anuales desde 2013.
- **INEI** (Censo / ENAPRES): tamaño de hogar, acceso y forma de abastecimiento por distrito.
- **SENAMHI — estación Juliaca**: temperatura mínima y máxima diaria, heladas, humedad relativa, histórico largo.
- **Acumulación propia de los nodos**: series de nivel, presión, caudal y temperatura de agua, cada 10 s.

**Fuente streaming.** Red de nodos propios ESP32 que publican vía **MQTT → Kafka** (1 nodo maestro con instrumentación completa + 7 nodos simples de nivel), escalada con una **flota sintética de 92 tanques** generada por un simulador con parámetros trazables a mediciones propias o datasets públicos. La plantilla del curso acepta simular la fuente streaming.

!!! info "Batch y streaming comparten origen físico"
    Los mismos nodos alimentan ambas capas: en vivo hacia la capa speed y por acumulación histórica hacia la capa batch. Ese es exactamente el patrón Lambda.

**Continuidad.** Este proyecto **no continúa un proyecto anterior**.

## 4. Aprobación

| Campo | Valor |
|---|---|
| Docente | |
| Fecha | |
