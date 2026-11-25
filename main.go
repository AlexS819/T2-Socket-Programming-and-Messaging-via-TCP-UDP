package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// shutdownTimeout — максимум часу, який даємо серверу на плавну зупинку.
const shutdownTimeout = 5 * time.Second

func main() {
	tcpAddr := flag.String("tcp", ":9000", "адреса TCP-слухача для клієнтів")
	udpAddr := flag.String("udp", ":9001", "адреса UDP-слухача для телеметрії")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags)

	fmt.Println("=== Мережевий сервіс диспетчеризації (Device & Dispatcher Gateway) ===")

	server := NewServer(*tcpAddr, *udpAddr, logger)
	if err := server.Start(); err != nil {
		logger.Printf("[СЕРВЕР] Помилка запуску: %v", err)
		os.Exit(1)
	}

	logger.Printf("[СЕРВЕР] TCP-слухач успішно відкрито на порту %s", port(server.TCPAddr()))
	logger.Printf("[СЕРВЕР] UDP-слухач телеметрії відкрито на порту %s", port(server.UDPAddr()))
	logger.Printf("[СЕРВЕР] Натисніть Ctrl+C для коректного завершення роботи")

	// Канал для сигналів ОС. Буфер 1, щоб signal.Notify не втратив
	// сигнал, якщо головна горутина ще не встигла його прочитати.
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopChan)

	<-stopChan

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Printf("[СЕРВЕР] Завершення роботи з помилкою: %v", err)
		os.Exit(1)
	}
}

// port повертає номер порту з адреси слухача (наприклад, ":9000" -> "9000").
func port(addr net.Addr) string {
	if addr == nil {
		return "?"
	}
	_, portNumber, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return portNumber
}
