# Big Data - Producto de Unidad 2

**Esta es la plantilla-ejemplo del producto de Unidad 2 de Big Data.** La estructura (ingesta de eventos empresariales e IoT, procesamiento streaming, observabilidad y costos, series de tiempo, BI/ML distribuido) es exigible a todos. El contenido usa el ejemplo de referencia del docente — cada equipo lo reemplaza por el de su propio instrumento, declarado en su [Brief técnico-analítico](brief.md) de S2.

!!! note "Contenido en construcción"
    Las sesiones S6-S11 (donde se construye cada capacidad de esta unidad) todavía no están publicadas. Esta plantilla ya fija la estructura exigible y la rúbrica, verificadas contra el sílabo; el detalle sesión por sesión del ejemplo (código, comandos exactos) se completa a medida que S6-S11 se publiquen — igual que hizo `u1-producto.md` con S1-S4.

## Producto

**Pipeline en tiempo real con ingesta de eventos empresariales e IoT/sensores, procesamiento streaming con Spark, observabilidad/costos y salidas BI/ML distribuidas.**

Implementa un pipeline Big Data en tiempo real que integra ingesta de eventos empresariales e IoT/sensores mediante Kafka, procesamiento distribuido con Spark Structured Streaming, observabilidad con Grafana y estimación de costos operacionales. Además, prepara salidas BI/ML distribuidas y reutiliza modelos para series de tiempo e inferencia batch y/o streaming.

## 1. Alcance de capacidades por sesión

**Tabla 1. De la sesión a la capacidad del sistema**

| Sesión | Capacidad que agrega | Ejemplo de referencia |
|---|---|---|
| S6 | Ingesta de eventos empresariales en tiempo real. | Un flujo de negocio (ej. ventas, transacciones) publicado y consumido con Kafka, con contrato de evento y tópico documentados. |
| S7 | Ingesta de eventos IoT/sensores en tiempo real. | Eventos de sensores o telemetría simulados, integrados al mismo pipeline de Kafka, con esquema, frecuencia y volumen ajustados. |
| S8 | Procesamiento streaming con Spark Structured Streaming. | Agregaciones en ventana con *watermarking* para datos tardíos y *checkpointing*, sobre el flujo de eventos de S6-S7. |
| S9 | Observabilidad con Grafana y estimación de costos. | Panel de observabilidad con métricas del pipeline streaming, umbrales, riesgos operativos y costo estimado de escalado. |
| S10 | Series de tiempo e inferencia en streaming. | Modelo o inferencia de series de tiempo aplicado sobre datos batch y/o eventos streaming del propio instrumento. |
| S11 | BI/ML distribuido: KPIs y visualización de la predicción. | Tablero BI con los KPIs del flujo de eventos en tiempo real, junto con la predicción de series de tiempo de S10, en la misma sesión. |
| S12 (esta evaluación) | Ensambla todo lo anterior en un solo pipeline y lo sustenta. | El pipeline completo de Unidad 2 + sección 4 de la guía de evaluación. |

Lo que sustentas en S12 es **tu propia dimensión U2**: las capacidades que tú construiste en S6-S11, sobre el instrumento de tu Proyecto Sello — no el ejemplo del docente.

## 2. Rúbrica de Evaluación

**Tabla 2. Rúbrica de evaluación de la Unidad 2**

| Criterio | Peso | CE / Nivel | A (20 pts) | B (15 pts) | C (10 pts) | D (5 pts) | Calificación obtenida |
|---|---:|---|---|---|---|---|---:|
| 1. Ingesta de eventos empresariales mediante Kafka | 12% | — (apoyo) | Eventos empresariales publicados y consumidos en vivo, con contrato de evento y tópico documentados. | Ingesta funcional, con documentación parcial del contrato. | Ingesta parcial o sin verificación en vivo. | No implementa ingesta de eventos empresariales. | |
| 2. Ingesta de eventos IoT/sensores | 10% | — (apoyo) | Eventos IoT/sensores integrados al mismo pipeline, con esquema y volumen ajustados y verificados. | Ingesta funcional, con ajustes menores pendientes. | Ingesta parcial o poco verificable. | No implementa ingesta de eventos IoT/sensores. | |
| 3. Procesamiento streaming con Spark Structured Streaming | 14% | — (apoyo) | Ventanas, *watermarking* y *checkpointing* verificados en vivo sobre datos reales del pipeline. | Procesamiento funcional, con verificación parcial de alguno de los tres elementos. | Procesamiento streaming presente, sin verificación clara. | No implementa procesamiento streaming. | |
| 4. Observabilidad con métricas y tableros | 12% | — (apoyo) | Panel de observabilidad operativo con métricas correlacionadas del pipeline streaming. | Panel operativo, con correlación parcial. | Métricas presentes, sin panel ni correlación clara. | No presenta observabilidad verificable. | |
| 5. Estimación de costos y criterios de escalado | 8% | — (apoyo) | Estimación de costos y criterios de escalado documentados y justificados frente al volumen real del pipeline. | Estimación presente, con justificación parcial. | Estimación mencionada, sin justificación clara. | No presenta estimación de costos. | |
| 6. Salidas BI/ML distribuidas | 12% | CE044-N3 | Tablero BI con KPIs del flujo en tiempo real, verificado en vivo. | Tablero funcional, con KPIs parciales. | Tablero presente, sin verificación clara. | No presenta salida BI/ML. | |
| 7. Series de tiempo o inferencia en streaming documentada y validada | 12% | CE043-N3 (completa) | Modelo o inferencia de series de tiempo validado con métricas, integrado al tablero BI (criterio 6). | Modelo o inferencia presente, con validación parcial. | Modelo mencionado, sin validación clara. | No presenta modelo ni inferencia de series de tiempo. | |
| 8. Sustentación | 20% | CG | Sustenta con claridad y profesionalismo su aporte individual, respondiendo con precisión las preguntas del jurado. | Sustenta con solvencia, con detalles menores en claridad, orden o precisión. | Sustenta con dificultad; claridad, orden o precisión insuficientes. | No sustenta adecuadamente ni demuestra su aporte individual. | |

Nota final = suma de (`Peso` × `Puntos de la calificación obtenida`) / 100 × 20.

`CE044-N3` = Nivel 3 de CE044 (Analiza y Define Estrategias). `CE043-N3 (completa)` = segunda mitad del Nivel 3 de CE043, que junto con el criterio 5 de Unidad 1 completa esa competencia. `— (apoyo)` = infraestructura técnica que hace posible generar la evidencia de nivel, sin ser en sí misma evidencia de una competencia. `CG` = Competencia General "Investigación e Innovación" del sílabo — no es una de las tres competencias CD/IA: los criterios 1-7 ya son la evidencia técnica, incluida su verificación en vivo; el criterio 8 verifica aporte individual y comunicación.

**Tabla 3. Subaspectos de la sustentación (Unidad 2)**

El criterio 8 se evalúa con los mismos 6 subaspectos de la sustentación integral del Proyecto Sello ([`u3-producto.md`](u3-producto.md#2-rubrica-de-evaluacion), Tabla 2) — exigibles desde esta sustentación de unidad, igual que en Unidad 1.

| Subaspecto | Qué observa en Unidad 2 |
|---|---|
| 1. Aporte individual | Cada integrante demuestra su propia dimensión U2 — no el trabajo del resto del equipo. |
| 2. Comunicación y orden | Claridad, estructura, tiempo y lenguaje técnico durante la presentación. |
| 3. Presentación personal y actitud | Puntualidad, vestimenta limpia y adecuada, higiene, cabello ordenado, actitud profesional, respeto, honestidad y coherencia con los valores y principios cristianos de la institución. |
| 4. Repositorio y estándares | Topics académicos vigentes, organización, commits y reproducibilidad del pipeline streaming. |
| 5. MkDocs o equivalente | Documentación de tu dimensión U2 publicada, navegable y alineada con `u2-producto.md`. |
| 6. Pitch/demo ejecutiva | Introducción breve de cómo evolucionó tu dimensión desde Unidad 1, con apoyo visual (.pptx, Canva o equivalente) — no reemplaza la demo técnica de la Tabla 2 de S12, la precede. |

Para usar la rúbrica con IA, solicita:

```text
Evalúa la sustentación y el producto (u2-producto.md, adaptada a la dimensión U2 propia del estudiante) usando la rúbrica de esta sección.
Para cada criterio selecciona la calificación obtenida: A=20, B=15, C=10, D=5.
Justifica brevemente cada nivel con evidencia concreta (eventos publicados/consumidos, panel de observabilidad, tablero BI, métricas del modelo).
Para el criterio 8, verifica explícitamente los 6 subaspectos de la Tabla 3 antes de asignar el nivel.
Calcula la nota final con la fórmula: suma de (Peso × Puntos de la calificación obtenida) / 100 × 20.
Indica 2 fortalezas y 2 recomendaciones para lo que sigue en Unidad III.
```

## 3. Trazabilidad y procedencia de la rúbrica

Los primeros siete criterios son cita literal de los criterios de evaluación del producto de la Unidad II en el sílabo de Big Data; el octavo (Sustentación) corresponde a la sustentación exigida por el mismo sílabo (sesión 12, actividad 2).

**Con la malla curricular:** el criterio 7 (series de tiempo, S10) completa la **segunda mitad del Nivel 3 de CE043** ("Genera Modelos") — la primera mitad (primer modelo entrenado y comparado) ya se evidenció en Unidad 1 (S4). El criterio 6 (BI/ML distribuido, S11) es la evidencia completa del **Nivel 3 de CE044** ("Analiza y Define Estrategias") — "visualiza la predicción de un modelo junto a KPIs del negocio en la misma sesión distribuida". Los criterios 1-5 (Kafka, streaming, observabilidad, costos) no se citan explícitamente en la definición de Nivel de ninguna de las tres competencias de CD/IA que aporta Big Data (CE042, CE043, CE044) — son la infraestructura técnica que hace posible generar esa evidencia, no evidencia de nivel por sí mismos. El criterio 8 (Sustentación) es transversal y no forma parte de la definición de ninguna competencia.
