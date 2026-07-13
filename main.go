package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// KVStore represents our thread-safe in-memory database
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore initializes the database
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair with an exclusive write lock
func (store *KVStore) Set(key, value string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = value
}

// Get retrieves a value by key using a shared read lock
func (store *KVStore) Get(key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	val, exists := store.data[key]
	return val, exists
}

// Delete removes a key safely
func (store *KVStore) Delete(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.data, key)
}

// AOF (Append-Only File) manages data persistence
type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

// NewAOF opens or creates the database.aof file
func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &AOF{
		file: f,
		rd:   bufio.NewReader(f),
	}, nil
}

// Close safely closes the AOF file
func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

// Write serializes a command array to RESP and appends it to the file
func (aof *AOF) Write(args []string) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Convert the command back into a RESP array
	res := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	_, err := aof.file.Write([]byte(res))
	if err != nil {
		return err
	}
	return aof.file.Sync() // Force write to physical disk immediately
}

// Read replays the AOF file to rebuild the database on startup
func (aof *AOF) Read(fn func(args []string)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart) // Rewind to the beginning of the file
	reader := bufio.NewReader(aof.file)

	for {
		args, err := parseRequest(reader)
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			return err // Some parsing error
		}
		fn(args) // Execute the parsed command
	}
	return nil
}

// readLine reads a single line up to the \r\n delimiter
func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

// parseRequest reads the RESP Array sent by official Redis clients
func parseRequest(reader *bufio.Reader) ([]string, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err // Connection closed or error
	}

	// Clients send commands as RESP Arrays (e.g., *3\r\n)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected RESP array")
	}

	numElements, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, numElements)

	for i := 0; i < numElements; i++ {
		// Read the bulk string length (e.g., $3\r\n)
		lenLine, err := readLine(reader)
		if err != nil || len(lenLine) == 0 || lenLine[0] != '$' {
			return nil, fmt.Errorf("expected RESP bulk string length")
		}

		// Read the actual string data (e.g., SET\r\n)
		strLine, err := readLine(reader)
		if err != nil {
			return nil, err
		}
		args[i] = strLine
	}

	return args, nil
}

// handleConnection processes client commands natively
func handleConnection(conn net.Conn, store *KVStore, aof *AOF) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := parseRequest(reader)
		if err != nil {
			return // Client disconnected or bad protocol
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])

		switch command {
		case "SET":
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			store.Set(args[1], args[2])
			aof.Write(args) // NEW: Persist command to disk
			// Reply with a RESP Simple String (+OK)
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			val, exists := store.Get(args[1])
			if exists {
				// Reply with a RESP Bulk String ($length\r\nvalue\r\n)
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
			} else {
				// Reply with RESP Null ($-1)
				conn.Write([]byte("$-1\r\n"))
			}

		case "DEL":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}
			store.Delete(args[1])
			aof.Write(args) // NEW: Persist command to disk
			// Reply with a RESP Integer (:1)
			conn.Write([]byte(":1\r\n"))

		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		default:
			// Reply with a RESP Error (-ERR)
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
		}
	}
}

func main() {
	port := 6379
	store := NewKVStore()

	// Initialize the Append-Only File
	aof, err := NewAOF("database.aof")
	if err != nil {
		log.Fatalf("Failed to open AOF: %s\n", err)
	}
	defer aof.Close()

	// RECOVERY PHASE: Replay the AOF to rebuild the database
	aof.Read(func(args []string) {
		command := strings.ToUpper(args[0])
		if command == "SET" {
			store.Set(args[1], args[2])
		} else if command == "DEL" {
			store.Delete(args[1])
		}
	})

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to bind to port %d: %s\n", port, err)
	}
	defer listener.Close()

	log.Printf("🔥 Mini-Redis (RESP Protocol) started on port %d\n", port)
	log.Println("Waiting for official Redis clients to connect...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s\n", err)
			continue
		}

		go handleConnection(conn, store, aof)
	}
}
