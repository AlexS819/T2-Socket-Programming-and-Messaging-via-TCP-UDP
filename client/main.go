// Тестовий клієнт для ручної перевірки сервера.
// Запуск: go run ./client -addr 127.0.0.1:9000
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "адреса TCP-сервера")
	flag.Parse()

	fmt.Println("Підключення до", *addr, "...")
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Println("Помилка підключення:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("З'єднання встановлено. Вводьте команди (CLOSE — вихід).")

	// Якщо stdin — не термінал (наприклад, конвеєр або файл), команди
	// не відображаються на екрані, тому дублюємо їх самі.
	echoInput := !isTerminal(os.Stdin)

	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		line := strings.TrimSpace(input.Text())
		if line == "" {
			continue
		}

		if echoInput {
			fmt.Println("> " + line)
		}

		if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
			fmt.Println("Помилка надсилання:", err)
			return
		}

		upper := strings.ToUpper(line)
		if upper == "CLOSE" || upper == "QUIT" {
			break
		}
	}

	// Даємо серверу час дописати відповіді, перш ніж закрити з'єднання.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

// isTerminal перевіряє, чи підключений файл до консолі (термінала).
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
