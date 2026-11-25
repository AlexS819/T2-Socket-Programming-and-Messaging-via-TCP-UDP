package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxLineSize — максимальна довжина одного рядка протоколу.
	// Захищає сервер від «безмежних» повідомлень від клієнта.
	maxLineSize = 4096

	// sendBuffer — розмір черги вихідних повідомлень одного клієнта.
	sendBuffer = 64

	// writeTimeout — скільки часу чекаємо на запис у сокет,
	// щоб повільний клієнт не заблокував обслуговуючу горутину назавжди.
	writeTimeout = 10 * time.Second

	// udpBufferSize — розмір буфера для однієї UDP-датаграми.
	udpBufferSize = 2048
)

// Client — одне активне TCP-підключення.
//
// Кожен клієнт має власну чергу вихідних повідомлень (send) і окрему
// горутину-письменника (writeLoop). Завдяки цьому розсилка не виконує
// мережевий запис під замком реєстру і не блокується на повільному клієнті.
type Client struct {
	conn net.Conn
	addr string

	send chan string

	// sendMu захищає sendOpen: закривати канал можна лише один раз
	// і лише тоді, коли в нього точно ніхто не пише.
	sendMu   sync.Mutex
	sendOpen bool

	writerDone chan struct{}
}

func newClient(conn net.Conn) *Client {
	return &Client{
		conn:       conn,
		addr:       conn.RemoteAddr().String(),
		send:       make(chan string, sendBuffer),
		sendOpen:   true,
		writerDone: make(chan struct{}),
	}
}

// enqueue ставить повідомлення в чергу клієнта без блокування.
// Якщо черга переповнена, повідомлення відкидається: краще втратити
// одне сповіщення для «завислого» клієнта, ніж заблокувати весь сервер.
func (c *Client) enqueue(message string) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if !c.sendOpen {
		return false
	}

	select {
	case c.send <- message:
		return true
	default:
		return false
	}
}

// closeSend закриває чергу. Після цього writeLoop дописує всі вже
// накопичені повідомлення та закриває з'єднання.
func (c *Client) closeSend() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if c.sendOpen {
		c.sendOpen = false
		close(c.send)
	}
}

// writeLoop — єдина горутина, яка пише в conn. Це виключає
// одночасний запис у сокет із кількох горутин (інтерлівінг байтів).
func (c *Client) writeLoop() {
	defer close(c.writerDone)
	defer c.conn.Close()

	for message := range c.send {
		_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := io.WriteString(c.conn, message+"\r\n"); err != nil {
			return
		}
	}
}

// Server — сервіс диспетчеризації: TCP-сервер команд і UDP-приймач телеметрії.
type Server struct {
	logger *log.Logger

	tcpAddr string
	udpAddr string

	tcpListener net.Listener
	udpConn     net.PacketConn

	// mu захищає clients. RWMutex дозволяє багатьом горутинам
	// одночасно читати реєстр (наприклад, для USERS), але зміна
	// складу клієнтів виконується ексклюзивно.
	mu      sync.RWMutex
	clients map[*Client]struct{}

	wg sync.WaitGroup

	shuttingDown atomic.Bool
	shutdownOnce sync.Once
}

func NewServer(tcpAddr, udpAddr string, logger *log.Logger) *Server {
	return &Server{
		logger:  logger,
		tcpAddr: tcpAddr,
		udpAddr: udpAddr,
		clients: make(map[*Client]struct{}),
	}
}

// Start відкриває обидва сокети та запускає обслуговуючі горутини.
func (s *Server) Start() error {
	tcpListener, err := net.Listen("tcp", s.tcpAddr)
	if err != nil {
		return fmt.Errorf("не вдалося відкрити TCP-слухач на %s: %w", s.tcpAddr, err)
	}

	udpConn, err := net.ListenPacket("udp", s.udpAddr)
	if err != nil {
		_ = tcpListener.Close()
		return fmt.Errorf("не вдалося відкрити UDP-слухач на %s: %w", s.udpAddr, err)
	}

	s.tcpListener = tcpListener
	s.udpConn = udpConn

	s.wg.Add(2)
	go s.acceptLoop()
	go s.telemetryLoop()

	return nil
}

// TCPAddr і UDPAddr повертають реальні адреси слухачів.
// Це корисно, коли в конфігурації вказано порт 0 (тести).
func (s *Server) TCPAddr() net.Addr {
	if s.tcpListener == nil {
		return nil
	}
	return s.tcpListener.Addr()
}

func (s *Server) UDPAddr() net.Addr {
	if s.udpConn == nil {
		return nil
	}
	return s.udpConn.LocalAddr()
}

// Wait блокується, доки всі обслуговуючі горутини не завершаться.
func (s *Server) Wait() {
	s.wg.Wait()
}

// acceptLoop приймає нові TCP-з'єднання. На кожне з'єднання
// запускається окрема горутина handleConn.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			if s.shuttingDown.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Printf("[СЕРВЕР] Помилка прийому з'єднання: %v", err)
			continue
		}

		if s.shuttingDown.Load() {
			_ = conn.Close()
			return
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// handleConn обслуговує одного TCP-клієнта: реєструє його,
// читає команди та прибирає за собою після розриву з'єднання.
func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()

	client := newClient(conn)
	go client.writeLoop()

	// Якщо сервер уже зупиняється, нового клієнта не реєструємо:
	// інакше він «загубився» б між перевіркою та додаванням у реєстр.
	if !s.addClient(client) {
		client.closeSend()
		<-client.writerDone
		return
	}
	defer s.removeClient(client)

	s.logger.Printf("[ПІДКЛЮЧЕННЯ] Новий клієнт підключився: %s", client.addr)

	client.enqueue("[СЕРВЕР] Ласкаво просимо до мережевого сервісу диспетчеризації!")
	client.enqueue("[СЕРВЕР] Ваша адреса: " + client.addr)
	client.enqueue("[СЕРВЕР] Введіть HELP, щоб отримати список команд.")

	s.broadcastSystem(fmt.Sprintf("[СИСТЕМА] Підключився новий клієнт: %s", client.addr), client)

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logger.Printf("[КЛІЄНТ %s] Помилка читання: %v", client.addr, err)
			}
			return
		}

		if len(line) > maxLineSize {
			client.enqueue("[ПОМИЛКА] Рядок задовгий. Максимум — 4096 символів.")
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if s.handleCommand(client, line) {
			return
		}
	}
}

// addClient додає клієнта до реєстру під ексклюзивним замком.
// Повертає false, якщо сервер уже завершує роботу.
func (s *Server) addClient(c *Client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shuttingDown.Load() {
		return false
	}

	s.clients[c] = struct{}{}
	return true
}

// removeClient гарантовано прибирає клієнта з реєстру,
// закриває його чергу та дочікується горутини-письменника.
func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()

	c.closeSend()
	<-c.writerDone

	if s.shuttingDown.Load() {
		return
	}

	s.logger.Printf("[ВІДКЛЮЧЕННЯ] Клієнт відключився: %s", c.addr)
	s.broadcastSystem(fmt.Sprintf("[СИСТЕМА] Клієнт відключився: %s", c.addr), c)
}

// broadcast розсилає повідомлення всім клієнтам, крім sender.
// Якщо sender == nil — розсилка йде абсолютно всім (телеметрія).
//
// Реєстр копіюється під RLock, а сам запис у клієнтські черги
// виконується вже без замка: інакше повільний клієнт зупинив би
// реєстрацію нових підключень і обробку команд.
func (s *Server) broadcast(sender *Client, message string) int {
	s.mu.RLock()
	targets := make([]*Client, 0, len(s.clients))
	for c := range s.clients {
		if c != sender {
			targets = append(targets, c)
		}
	}
	s.mu.RUnlock()

	sent := 0
	for _, c := range targets {
		if c.enqueue(message) {
			sent++
		}
	}
	return sent
}

func (s *Server) broadcastSystem(message string, exclude *Client) {
	s.broadcast(exclude, message)
}

// telemetryLoop приймає UDP-датаграми та транслює їх TCP-клієнтам.
func (s *Server) telemetryLoop() {
	defer s.wg.Done()

	buffer := make([]byte, udpBufferSize)
	for {
		n, addr, err := s.udpConn.ReadFrom(buffer)
		if err != nil {
			if s.shuttingDown.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Printf("[UDP] Помилка читання датаграми: %v", err)
			continue
		}

		data := strings.TrimSpace(string(buffer[:n]))
		if data == "" {
			continue
		}

		s.logger.Printf("[UDP ТЕЛЕМЕТРІЯ] Отримано %d байт від %s -> %q", n, addr, data)
		s.broadcast(nil, fmt.Sprintf("[TELEMETRY from %s]: %s", addr, data))
	}
}

// Shutdown плавно зупиняє сервер: закриває слухачі, надсилає клієнтам
// фінальне сповіщення, закриває з'єднання та чекає на горутини.
func (s *Server) Shutdown(ctx context.Context) error {
	var closeErr error

	s.shutdownOnce.Do(func() {
		s.shuttingDown.Store(true)
		s.logger.Printf("[СЕРВЕР] Отримано сигнал завершення роботи. Запуск Graceful Shutdown...")

		if s.tcpListener != nil {
			if err := s.tcpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErr = err
			}
		}
		if s.udpConn != nil {
			if err := s.udpConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErr = err
			}
		}

		s.mu.Lock()
		count := len(s.clients)
		for c := range s.clients {
			c.enqueue("[СИСТЕМА]: SERVER_SHUTTING_DOWN")
			c.closeSend()
		}
		s.mu.Unlock()

		s.logger.Printf("[СЕРВЕР] Сповіщено активних клієнтів: %d. З'єднання безпечно закриті.", count)

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			s.logger.Printf("[СЕРВЕР] Роботу сервера успішно завершено.")
		case <-ctx.Done():
			if closeErr == nil {
				closeErr = ctx.Err()
			}
		}
	})

	return closeErr
}
