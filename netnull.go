package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	humanize "github.com/dustin/go-humanize"
)

var blockSizeKilobytes = flag.Uint("blocksize", 256, "send blocksize in Kilobytes")
var bytesDisplayFlag = flag.Bool("bytes", false,
	"display results in bytes (normally shown in 'humanized' form)")
var halfClose = flag.Bool("halfclose", false,
	"the unused direction of a send- or receive-only connection should be closed")
var listenAddr = flag.String("addr", "*", "server listen address")
var listenPort = flag.Uint("port", 2021, "server listen port")
var receiveFlag = flag.Bool("receive", false,
	"the server should receive data from clients")
var sendFlag = flag.Bool("send", false, "the server should send data to clients")
var verbose = flag.Bool("verbose", false, "print additional diagnostic information")

func fixformat(format string) string {
	if !(format[len(format)-1] == '\n') {
		format += "\n"
	}
	return format
}

func iprintf(format string, args ...interface{}) {
	fmt.Printf(fixformat(format), args...)
}

func vprintf(format string, args ...interface{}) {
	format = fixformat(format)
	if *verbose {
		fmt.Printf(format, args...)
	}
}

var writeData []byte

func acceptHandler(conn net.Conn) {
	defer conn.Close()

	cinfo := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())
	var sent, received uint64
	var wg sync.WaitGroup

	// Reader side.
	if !*receiveFlag {
		if *halfClose {
			// We fully expect conn's concrete type to be *net.TCPConn.
			switch conn := conn.(type) {
			case *net.TCPConn:
				vprintf("[%s] Input: Closing socket for read", cinfo)
				err := (*conn).CloseRead()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[%s] Input: CloseRead(): %s", err)
				}
			}
		}
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, (*blockSizeKilobytes)*1024)
			input := bufio.NewReader(conn)
			for {
				n, err := input.Read(data)

				if err == io.EOF {
					// This is ok.
					vprintf("[%s] Input: Received EOF\n", cinfo)
					break
				} else if err != nil {
					fmt.Fprintf(os.Stderr, "[%s] Input: error: %s", cinfo, err)
					break
				}

				received += uint64(n)
				vprintf("[%s] Input: Received %d bytes\n", cinfo, n)
			}
		}()
	}

	// Writer side.
	if !*sendFlag {
		if *halfClose {
			switch conn := conn.(type) {
			case *net.TCPConn:
				vprintf("[%s] Output: Closing socket for write", cinfo)
				err := (*conn).CloseWrite()
				if err != nil {
					fmt.Fprintf(os.Stderr, "[%s] Output: CloseWrite(): %s", err)
				}
			}
		}
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			output := bufio.NewWriter(conn)
			for {
				n, err := output.Write(writeData)
				if err == io.EOF {
					// This is ok.
					vprintf("[%s] Output: Received EOF\n", cinfo)
					break
				} else if err != nil {
					// 'Broken pipe' is the most likely error here.
					fmt.Fprintf(os.Stderr, "[%s] Output: error: %s\n", cinfo, err)
					break
				}
				sent += uint64(n)
				vprintf("[%s] Output: Wrote %d bytes\n", cinfo, n)
			}
		}()
	}

	wg.Wait()
	iprintf("[%s] Closing connection: sent %s, received %s\n",
		cinfo, humanize.Bytes(sent), humanize.Bytes(received))
}

func main() {
	flag.Parse()

	if !*sendFlag && !*receiveFlag {
		log.Fatalf("You must use at least one of -send and -receive!")
	}

	writeData = make([]byte, (*blockSizeKilobytes)*1024)

	if *listenAddr == "*" {
		*listenAddr = "0.0.0.0" // XXX Assumes IPv4.
	}

	laddr := fmt.Sprintf("%s:%v", *listenAddr, *listenPort)
	iprintf("Listening on %s\n", laddr)
	listener, err := net.Listen("tcp", laddr)

	if err != nil {
		log.Fatalf("Couldn't open listen socket for %s: %s",
			laddr, err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Accept() failed: %s", err)
		}
		iprintf("Accepted connection from %s\n", conn.RemoteAddr())
		go acceptHandler(conn)
	}
}
