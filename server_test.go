package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// startTestServer піднімає сервер на вільних портах (порт 0).
func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()

	logger := log.New(io.Discard, "", 0)
	server := NewServer("127.0.0.1:0", "127.0.0.1:0", logger)

	if err := server.Start(); err != nil {
		t.Fatalf("не вдалося запустити тестовий сервер: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	return server, server.TCPAddr().String()
}

func dialTCP(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("не вдалося підключитися до %s: %v", addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("не вдалося встановити дедлайн: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

func sendLine(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
		t.Fatalf("не вдалося надіслати %q: %v", line, err)
	}
}

// readUntil читає рядки, доки не зустріне підрядок substr.
func readUntil(t *testing.T, reader *bufio.Reader, substr string) string {
	t.Helper()

	var received strings.Builder
	for i := 0; i < 100; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("очікували %q, але з'єднання закрилося (%v). Отримано:\n%s",
				substr, err, received.String())
		}
		received.WriteString(line)
		if strings.Contains(line, substr) {
			return received.String()
		}
	}

	t.Fatalf("не дочекалися %q. Отримано:\n%s", substr, received.String())
	return ""
}

func readUntilEOF(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	var received strings.Builder
	for i := 0; i < 100; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return received.String()
		}
		received.WriteString(line)
	}

	t.Fatal("сервер не закрив з'єднання")
	return ""
}

func TestEchoTimeAndUnknownCommand(t *testing.T) {
	_, tcpAddr := startTestServer(t)
	conn, reader := dialTCP(t, tcpAddr)

	readUntil(t, reader, "Введіть HELP")

	sendLine(t, conn, "ECHO Тестова перевірка")
	if got := readUntil(t, reader, "[ECHO]:"); !strings.Contains(got, "[ECHO]: Тестова перевірка") {
		t.Fatalf("некоректна відповідь ECHO: %q", got)
	}

	sendLine(t, conn, "TIME")
	got := readUntil(t, reader, "[TIME]:")
	value := strings.TrimSpace(strings.SplitN(got, "[TIME]:", 2)[1])
	if _, err := time.Parse(time.RFC3339, value); err != nil {
		t.Fatalf("TIME повернув не RFC3339: %q (%v)", value, err)
	}

	sendLine(t, conn, "NOT_A_COMMAND")
	readUntil(t, reader, "Невідома команда")

	sendLine(t, conn, "CLOSE")
	readUntil(t, reader, "До побачення")
	readUntilEOF(t, reader)
}

func TestBroadcastBetweenClients(t *testing.T) {
	_, tcpAddr := startTestServer(t)

	first, firstReader := dialTCP(t, tcpAddr)
	readUntil(t, firstReader, "Введіть HELP")

	_, secondReader := dialTCP(t, tcpAddr)
	readUntil(t, secondReader, "Введіть HELP")

	// Перший клієнт має отримати системне сповіщення про нового учасника.
	readUntil(t, firstReader, "Підключився новий клієнт")

	sendLine(t, first, "BROADCAST Усім привіт у диспетчерській!")

	got := readUntil(t, secondReader, "[СПОВІЩЕННЯ від")
	if !strings.Contains(got, "Усім привіт у диспетчерській!") {
		t.Fatalf("другий клієнт не отримав текст розсилки: %q", got)
	}

	confirmation := readUntil(t, firstReader, "Повідомлення надіслано клієнтам:")
	if !strings.Contains(confirmation, "надіслано клієнтам: 1") {
		t.Fatalf("некоректне підтвердження розсилки: %q", confirmation)
	}
}

func TestUsersCommandSeesBothClients(t *testing.T) {
	_, tcpAddr := startTestServer(t)

	first, firstReader := dialTCP(t, tcpAddr)
	readUntil(t, firstReader, "Введіть HELP")

	_, secondReader := dialTCP(t, tcpAddr)
	readUntil(t, secondReader, "Введіть HELP")
	readUntil(t, firstReader, "Підключився новий клієнт")

	sendLine(t, first, "USERS")
	got := readUntil(t, firstReader, "(ви)")
	if !strings.Contains(got, "Активні клієнти (2)") {
		t.Fatalf("очікували двох клієнтів у списку: %q", got)
	}
}

func TestTelemetryBroadcastFromUDP(t *testing.T) {
	server, tcpAddr := startTestServer(t)

	_, reader := dialTCP(t, tcpAddr)
	readUntil(t, reader, "Введіть HELP")

	udpConn, err := net.Dial("udp", server.UDPAddr().String())
	if err != nil {
		t.Fatalf("не вдалося створити UDP-клієнт: %v", err)
	}
	defer udpConn.Close()

	if _, err := udpConn.Write([]byte("temp=24.5;press=750\n")); err != nil {
		t.Fatalf("не вдалося надіслати датаграму: %v", err)
	}

	got := readUntil(t, reader, "[TELEMETRY from")
	if !strings.Contains(got, "temp=24.5;press=750") {
		t.Fatalf("телеметрія не дійшла до TCP-клієнта: %q", got)
	}
}

// TestConcurrentClientsStress перевіряє, що одночасні підключення,
// розсилки та відключення не призводять до паніки або взаємного блокування.
func TestConcurrentClientsStress(t *testing.T) {
	server, tcpAddr := startTestServer(t)

	const clients = 10
	var wg sync.WaitGroup

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", tcpAddr)
			if err != nil {
				return
			}
			defer conn.Close()

			// Постійно вичитуємо відповіді, щоб не переповнити чергу клієнта.
			go func() {
				_, _ = io.Copy(io.Discard, conn)
			}()

			for j := 0; j < 5; j++ {
				fmt.Fprintf(conn, "ECHO клієнт=%d ітерація=%d\r\n", id, j)
				fmt.Fprintf(conn, "BROADCAST масове повідомлення %d/%d\r\n", id, j)
				fmt.Fprintf(conn, "USERS\r\n")
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Паралельно «шумимо» UDP-датаграмами.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := net.Dial("udp", server.UDPAddr().String())
		if err != nil {
			return
		}
		defer conn.Close()
		for i := 0; i < 20; i++ {
			fmt.Fprintf(conn, "temp=%d.5;press=%d\r\n", 20+i, 740+i)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown після навантаження повернув помилку: %v", err)
	}
}

func TestGracefulShutdownNotifiesClients(t *testing.T) {
	server, tcpAddr := startTestServer(t)

	_, reader := dialTCP(t, tcpAddr)
	readUntil(t, reader, "Введіть HELP")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown повернув помилку: %v", err)
	}

	got := readUntil(t, reader, "SERVER_SHUTTING_DOWN")
	if !strings.Contains(got, "[СИСТЕМА]: SERVER_SHUTTING_DOWN") {
		t.Fatalf("клієнт не отримав фінальне сповіщення: %q", got)
	}
	readUntilEOF(t, reader)

	if _, err := net.DialTimeout("tcp", tcpAddr, 200*time.Millisecond); err == nil {
		t.Fatal("після Shutdown сервер не повинен приймати нові підключення")
	}
}
