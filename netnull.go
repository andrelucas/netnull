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

func alwaysNewline(format string) string {
	if !(format[len(format)-1] == '\n') {
		format += "\n"
	}
	return format
}

func iprintf(format string, args ...interface{}) {
	fmt.Printf(alwaysNewline(format), args...)
}

func vprintf(format string, args ...interface{}) {
	if *verbose {
		fmt.Printf(alwaysNewLine(format), args...)
	}
}

var writeData []byte

func maybeHumanBytes(b uint64) string {
	if !*bytesDisplayFlag {
		return humanize.Bytes(b)
	} else {
		return fmt.Sprintf("%v bytes", b)
	}
}

func readLoop(conn net.Conn, wg *sync.WaitGroup) {

	cinfo := fmt.Sprintf("[%s->%s] Input:", conn.RemoteAddr(), conn.LocalAddr())
	var received uint64

	defer func() {
		iprintf("%s Received %s total", cinfo, maybeHumanBytes(received))
		wg.Done()
	}()

	data := make([]byte, (*blockSizeKilobytes)*1024)

	input := bufio.NewReader(conn)

	for {
		n, err := input.Read(data)

		if err == io.EOF {
			// This is ok.
			vprintf("%s Received EOF\n", cinfo)
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "%s error: %s", cinfo, err)
			break
		}

		received += uint64(n)
		vprintf("%s Received %d bytes\n", cinfo, n)
	}
}

func writeLoop(conn net.Conn, wg *sync.WaitGroup) {

	cinfo := fmt.Sprintf("[%s->%s] Output:", conn.RemoteAddr(), conn.LocalAddr())
	var sent uint64

	defer func() {
		iprintf("%s Sent %s total", cinfo, maybeHumanBytes(sent))
		wg.Done()
	}()

	output := bufio.NewWriter(conn)

WRITE:
	for {
		n, err := output.Write(writeData)
		if err == io.EOF {
			// This is ok, but unlikely - 'broken pipe' is more likely.
			vprintf("%s Received EOF\n", cinfo)
			break
		} else if err != nil {
			// 'Broken pipe' is the most likely error here. Handle it specially.
			switch err := err.(type) {
			case *net.OpError:
				// Ugly string match.
				if fmt.Sprint(err.Err) == "write: broken pipe" {
					vprintf("%s Remote closed the connection", cinfo)
					break WRITE
				}
			}
			// Ok, this really is an unexpected error.
			fmt.Fprintf(os.Stderr, "%s error: %s\n", cinfo, err)
			break
		}
		sent += uint64(n)
		vprintf("%s Wrote %d bytes\n", cinfo, n)
	}
}

func acceptHandler(conn net.Conn) {

	cinfo := fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr())
	defer func() {
		iprintf("[%s] Closing connection", cinfo)
		conn.Close()
	}()

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
		go readLoop(conn, &wg)
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
		go writeLoop(conn, &wg)
	}

	wg.Wait()
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
