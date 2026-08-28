# Observatorio domiciliario del servicio de agua de Juliaca — MonitorAqua

**Proyecto Sello · Equipo Horeb · Curso Big Data**

> **¿Cómo anticipar la disponibilidad y la calidad del agua en los hogares de Juliaca que dependen de tanques de almacenamiento a causa del suministro intermitente?**

## El problema en cuatro líneas

En Juliaca el agua potable llega de forma intermitente, por lo que los hogares dependen de tanques de almacenamiento para cubrir las horas sin servicio.
Nadie sabe con certeza cuánta agua le queda a su tanque, cuándo volverá a llegar el agua, ni cuánta se pierde por fugas invisibles.
Además, el agua almacenada durante horas o días en el frío del altiplano cambia de temperatura y pierde cloro residual, comprometiendo su calidad.
MonitorAqua instala sensores en los tanques y responde estas preguntas con una arquitectura Big Data de tipo Lambda: histórico batch y tiempo real en streaming, convergiendo en un solo tablero.

## Páginas del sitio

- **[Brief](brief.md)** — Datos del equipo, dominio del proyecto, usuarios, arquitectura elegida y fuentes de datos.
- **[Arquitectura](arquitectura.md)** — Diagrama Lambda completo, nodo maestro, tecnologías por etapa, estado real del despliegue, esquemas JSON y el flujo de un dato de extremo a extremo.
- **[Dimensiones](dimensiones.md)** — Las 8 dimensiones del proyecto (4 en batch U1, 4 en streaming U2), con sus fichas completas, alcance y trazabilidad del simulador.
- **[Datos](datos.md)** — Ficha del dataset de muestra `tanques_horeb_muestra.csv`.
