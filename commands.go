package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// splitCommand розділяє рядок протоколу на команду та аргумент.
// Команда наводиться до верхнього регістру, аргумент зберігається як є.
func splitCommand(line string) (command string, argument string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", ""
	}

	command, argument, _ = strings.Cut(line, " ")
	return strings.ToUpper(strings.TrimSpace(command)), strings.TrimSpace(argument)
}

// handleCommand виконує одну команду протоколу.
// Повертає true, якщо після цієї команди з'єднання треба закрити.
func (s *Server) handleCommand(client *Client, line string) bool {
	command, argument := splitCommand(line)

	switch command {
	case "ECHO":
		if argument == "" {
			client.enqueue("[ПОМИЛКА] Використання: ECHO <повідомлення>")
			return false
		}
		client.enqueue("[ECHO]: " + argument)

	case "TIME":
		client.enqueue("[TIME]: " + time.Now().Format(time.RFC3339))

	case "BROADCAST", "MSG":
		if argument == "" {
			client.enqueue("[ПОМИЛКА] Використання: BROADCAST <повідомлення>")
			return false
		}

		s.logger.Printf("[РОЗСИЛКА] Від %s -> %q", client.addr, argument)
		sent := s.broadcast(client, fmt.Sprintf("[СПОВІЩЕННЯ від %s]: %s", client.addr, argument))
		client.enqueue(fmt.Sprintf("[СИСТЕМА] Повідомлення надіслано клієнтам: %d", sent))

	case "USERS":
		s.handleUsers(client)

	case "HELP":
		client.enqueue("[СЕРВЕР] Доступні команди:")
		client.enqueue("  ECHO <текст>       — повернути текст назад")
		client.enqueue("  TIME               — поточний час сервера (RFC3339)")
		client.enqueue("  BROADCAST <текст>  — надіслати текст усім іншим клієнтам")
		client.enqueue("  USERS              — список активних клієнтів")
		client.enqueue("  HELP               — ця довідка")
		client.enqueue("  CLOSE | QUIT       — завершити з'єднання")

	case "CLOSE", "QUIT":
		client.enqueue("[СЕРВЕР] До побачення! З'єднання буде закрито.")
		return true

	case "":
		client.enqueue("[ПОМИЛКА] Порожня команда. Введіть HELP.")

	default:
		client.enqueue(fmt.Sprintf("[ПОМИЛКА] Невідома команда: %q", command))
		client.enqueue("[ПОМИЛКА] Доступні команди: ECHO, TIME, BROADCAST, USERS, HELP, CLOSE")
	}

	return false
}

// handleUsers показує список підключених клієнтів.
// Реєстр копіюється під RLock, тому відповідь формується
// без блокування інших горутин на час сортування та відправки.
func (s *Server) handleUsers(client *Client) {
	s.mu.RLock()
	addresses := make([]string, 0, len(s.clients))
	for c := range s.clients {
		addresses = append(addresses, c.addr)
	}
	s.mu.RUnlock()

	sort.Strings(addresses)

	client.enqueue(fmt.Sprintf("[СИСТЕМА] Активні клієнти (%d):", len(addresses)))
	for _, addr := range addresses {
		if addr == client.addr {
			client.enqueue("  - " + addr + " (ви)")
			continue
		}
		client.enqueue("  - " + addr)
	}
}
