package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ==========================================
// 1. INTERFACES Y ERRORES DEL SISTEMA
// ==========================================

// Transmisible define el comportamiento polimórfico de cualquier contenido multimedia.
type Transmisible interface {
	ObtenerInfoEmision() string
	GenerarSiguienteChunk(secuencia int) ([]byte, error)
}

// Alertas de error de infraestructura de streaming
var (
	ErrArchivoCorrupto = errors.New("error crítico: el fragmento de video en disco está corrupto")
	ErrSenalPerdida    = errors.New("error de red: se perdió la señal de origen (Live Ingest)")
)

// ==========================================
// 2. ENCAPSULACIÓN Y ESTRUCTURAS (POO)
// ==========================================

type RecursoMultimedia struct {
	id          string
	titulo      string
	duracionSeg int
	bitrateKbps int
}

type VideoVOD struct {
	RecursoMultimedia
	codec         string
	tamanoTotalMB float64
}

// Constructor seguro para Video bajo demanda
func NuevoVideoVOD(id, titulo string, duracion, bitrate int, codec string, tamano float64) VideoVOD {
	return VideoVOD{
		// Corrección: Cambiado 'duration' por 'duracion'
		RecursoMultimedia: RecursoMultimedia{id: id, titulo: titulo, duracionSeg: duracion, bitrateKbps: bitrate},
		codec:             codec,
		tamanoTotalMB:     tamano,
	}
}

type StreamingEnVivo struct {
	RecursoMultimedia
	plataformaIngesta string
	espectadores      int
}

// Constructor seguro para Transmisiones en Vivo
func NuevoStreamingEnVivo(id, titulo string, bitrate int, plataforma string, espectadores int) StreamingEnVivo {
	return StreamingEnVivo{
		RecursoMultimedia: RecursoMultimedia{id: id, titulo: titulo, duracionSeg: 0, bitrateKbps: bitrate},
		plataformaIngesta: plataforma,
		espectadores:      espectadores,
	}
}

func (rm RecursoMultimedia) ObtenerInfoEmision() string {
	return fmt.Sprintf("[%s] '%s' (Calidad: %d Kbps)", rm.id, rm.titulo, rm.bitrateKbps)
}

// ==========================================
// 3. SERIALIZACIÓN JSON (Estructuras de Red / DTOs)
// ==========================================

// ChunkDTO (Data Transfer Object) define la estructura exacta que viajará por la red en formato JSON.
// Usamos "tags" de JSON (`json:"..."`) para formatear las llaves según los estándares web.
type ChunkDTO struct {
	Secuencia      int    `json:"sequence_id"`
	ContenidoID    string `json:"content_id"`
	Payload        string `json:"payload_data"`
	TimestampEnvio string `json:"transmitted_at"`
}

// GenerarSiguienteChunk para VideoVOD con salida lista para Serializar
func (v VideoVOD) GenerarSiguienteChunk(secuencia int) ([]byte, error) {
	if rand.Float32() < 0.04 { // 4% de probabilidad de falla en disco
		return nil, ErrArchivoCorrupto
	}

	// Creamos el objeto intermedio que representa el paquete de red
	paquete := ChunkDTO{
		Secuencia:      secuencia,
		ContenidoID:    v.id,
		Payload:        fmt.Sprintf("DATA-VOD-RAW-[%s]-[Codec:%s]", v.titulo, v.codec),
		TimestampEnvio: time.Now().Format("15:04:05.000"),
	}

	// SERIALIZACIÓN: Convertimos la estructura de Go en bytes JSON estructurados
	jsonBytes, err := json.Marshal(paquete)
	if err != nil {
		return nil, fmt.Errorf("falla de serialización JSON: %w", err)
	}

	return jsonBytes, nil
}

// GenerarSiguienteChunk para StreamingEnVivo con salida lista para Serializar
func (e StreamingEnVivo) GenerarSiguienteChunk(secuencia int) ([]byte, error) {
	if rand.Float32() < 0.08 { // 8% de probabilidad de pérdida de señal
		return nil, ErrSenalPerdida
	}

	paquete := ChunkDTO{
		Secuencia:      secuencia,
		ContenidoID:    e.id,
		Payload:        fmt.Sprintf("DATA-LIVE-STREAM-[Viewers:%d]-[Signal:%s]", e.espectadores, e.plataformaIngesta),
		TimestampEnvio: time.Now().Format("15:04:05.000"),
	}

	// SERIALIZACIÓN: Convertimos la estructura a JSON
	jsonBytes, err := json.Marshal(paquete)
	if err != nil {
		return nil, fmt.Errorf("falla de serialización JSON: %w", err)
	}

	return jsonBytes, nil
}

// ==========================================
// 4. MOTOR CONCURRENTE DE STREAMING (Channels)
// ==========================================

type ServidorStreaming struct {
	CapacidadMbps float64
}

func (ss *ServidorStreaming) TransmitirContenido(usuarioID string, contenido Transmisible, chunksTotales int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Canal con Buffer que transporta exclusivamente cadenas de texto en formato JSON (`[]byte`)
	canalBufferRed := make(chan []byte, 2)
	canalError := make(chan error, 1)

	// GOROUTINE PRODUCTORA (Servidor emitiendo y serializando datos)
	go func() {
		defer close(canalBufferRed)
		defer close(canalError)

		for i := 1; i <= chunksTotales; i++ {
			jsonChunk, err := contenido.GenerarSiguienteChunk(i)
			if err != nil {
				canalError <- err
				return
			}

			canalBufferRed <- jsonChunk // Insertamos el JSON puro en el buffer de red
			time.Sleep(80 * time.Millisecond)
		}
	}()

	// HILO CONSUMIDOR (El dispositivo del Cliente que recibe el JSON y lo deserializa)
	for {
		select {
		case err, ok := <-canalError:
			if ok && err != nil {
				fmt.Printf("\n[ALERTA - CLIENTE %s] Error de red detectado: %s\n", usuarioID, err)
				return
			}

		case jsonRaw, abierto := <-canalBufferRed:
			if !abierto {
				fmt.Printf("\n[✓] Transmisión JSON finalizada con éxito para Cliente: %s\n", usuarioID)
				return
			}

			// IMPRESIÓN DEL TEXTO EN FORMATO JSON QUE VIAJA POR LA RED
			fmt.Printf("\n[RED -> %s] Recibido Raw JSON: %s\n", usuarioID, string(jsonRaw))

			// DESERIALIZACIÓN: El dispositivo del cliente decodifica el JSON de vuelta a un objeto nativo
			var paqueteRecibido ChunkDTO
			err := json.Unmarshal(jsonRaw, &paqueteRecibido)
			if err != nil {
				fmt.Printf("[Error Cliente] No se pudo decodificar el paquete JSON: %s\n", err)
				return
			}

			// El cliente interactúa con los datos ya recuperados del JSON
			fmt.Printf("   └─> [REPRODUCTOR %s] Procesando Secuencia %d | Enviado a las: %s | Payload decodificado: %s\n",
				usuarioID, paqueteRecibido.Secuencia, paqueteRecibido.TimestampEnvio, paqueteRecibido.Payload)

			time.Sleep(100 * time.Millisecond)
		}
	}
}

// ==========================================
// 5. ESCENARIO DE EJECUCIÓN GENERAL
// ==========================================

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("=== BIOBOOKENGINE: MÓDULO 2 - MOTOR COMPLETO CON PARSING JSON ===")
	fmt.Println("-----------------------------------------------------------------")

	// Creación de objetos mediante encapsulamiento
	pelicula := NuevoVideoVOD("VOD-800", "Introducción a Kubernetes", 90, 4800, "AV1 Codec", 450.2)
	conferenciaLive := NuevoStreamingEnVivo("LIVE-400", "Keynote: El Futuro del Software 2026", 3200, "SRT Protocol", 1250)

	servidor := ServidorStreaming{CapacidadMbps: 1000.0}
	var wg sync.WaitGroup

	fmt.Println(">>> Catálogo Activo para Emisión:")
	fmt.Println("[CATÁLOGO]", pelicula.ObtenerInfoEmision())
	fmt.Println("[CATÁLOGO]", conferenciaLive.ObtenerInfoEmision())
	fmt.Println("-----------------------------------------------------------------")

	// Disparamos las transmisiones simultáneas
	wg.Add(2)
	go servidor.TransmitirContenido("Usuario-Carlos", pelicula, 3, &wg)
	go servidor.TransmitirContenido("Usuario-Maria", conferenciaLive, 3, &wg)

	wg.Wait()
	fmt.Println("\n-----------------------------------------------------------------")
	fmt.Println("Ecosistema de streaming apagado limpiamente. Todos los buffers vacíos.")
}
