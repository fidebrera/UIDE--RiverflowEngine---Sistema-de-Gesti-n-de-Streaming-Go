# RiverflowEngine - Módulo 2: Sistema de Gestión de Streaming

## Datos del Grupo
* **Institución:** UIDE - Facultad de Ingeniería de Sistemas
* **Curso:** Programación Orientada a Objetos
* **Integrante:** JUAN FIDEL CABRERA MENESES
* **Fecha:** 28 de Junio de 2026

## Objetivo del Programa
El objetivo principal de este software es diseñar e implementar un motor de procesamiento y distribución de streaming multimedia (Video bajo demanda - VOD y Transmisiones en Vivo - LIVE) utilizando el lenguaje Go (Golang). El programa demuestra la aplicación de conceptos modernos de la Programación Orientada a Objetos (POO) como la composición de estructuras (`struct embedding`), encapsulamiento por visibilidad de paquetes e interfaces implícitas, integrando robustez mediante un sistema estructurado de manejo de errores y un motor síncrono multitarea de alta concurrencia basado en canales con buffer (`buffered channels`) y subrutinas (`goroutines`).

## Explicación de las Principales Funcionalidades

1. **Encapsulamiento de Datos y Constructores Seguros:** Las propiedades de los recursos multimedia (como IDs, títulos y tasas de bits) se declaran con identificadores no exportados (minúsculas). El acceso y la creación de objetos se restringen exclusivamente a través de funciones constructoras como `NuevoVideoVOD()` y `NuevoStreamingEnVivo()`.
2. **Polimorfismo mediante Interfaces:** Se define la interfaz `Transmisible` con los métodos `ObtenerInfoEmision()` y `GenerarSiguienteChunk()`. Tanto el contenido pregrabado como el contenido en vivo implementan esta interfaz de forma implícita, permitiendo al servidor procesar cualquier flujo multimedia de manera genérica.
3. **Manejo Idiomático de Errores en Red:** Cada fragmento de video (`chunk`) se genera validando posibles fallos. El sistema simula caídas de red y archivos corruptos como valores de error ordinarios (`error`), permitiendo al software recuperarse o abortar conexiones específicas sin detener el servidor global.
4. **Motor de Transmisión con Buffering Concurrente:** Utiliza un patrón Productor-Consumidor implementado con una `goroutine` que lee los fragmentos por adelantado y los deposita en un canal con almacenamiento intermedio (`chan []byte, 2`). Un bucle supervisor utiliza la sentencia `select` para consumir simultáneamente el video y escuchar alertas críticas de errores, replicando exactamente el comportamiento de plataformas como YouTube o Netflix.
