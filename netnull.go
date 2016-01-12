// Source and sink server for testing.
//
// Optionally can write files, opts for speed rather than caring
// about space, so this can be risky if used on systems where
// filling the disk is bad.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"sync"
	"time"

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
var writeFileFlag = flag.Bool("write-file", false,
	"write received data to the filesystem (dangerous!)")
var writeFileChunkMB = flag.Uint64("write-file-chunk-mb", 500,
	"maximum size of written file in MB - larger input will wrap around but will not fail.\n"+
		"\tNote that this will mean a file of this size per connection, so it will be trivial\n"+
		"\tto fill your target filesystem!")
var writeFilePath = flag.String("write-file-prefix", "/var/tmp/netnull-tmp",
	"prefix of written files - will have a random string appended")
var writeFileNoDeleteFlag = flag.Bool("write-file-no-delete", false,
	"do not delete written files (dangerous!)")

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
		fmt.Printf(alwaysNewline(format), args...)
	}
}

var netOutputData []byte

func maybeHumanBytes(b uint64) string {
	if !*bytesDisplayFlag {
		return humanize.Bytes(b)
	} else {
		return fmt.Sprintf("%v bytes", b)
	}
}

func writeToChunkFile(file io.Writer, data *[]byte, size uint) {

}

func readLoop(conn net.Conn, wg *sync.WaitGroup) {

	cinfo := fmt.Sprintf("[%s->%s] Input:", conn.RemoteAddr(), conn.LocalAddr())
	var received uint64
	var writeFile *os.File
	var writeFileName string
	var writeFileOffset uint64
	var writeWrapCount uint
	var writeFileSize uint64 = *writeFileChunkMB * uint64(1000*1000)

	defer wg.Done()

	data := make([]byte, (*blockSizeKilobytes)*1024)

	input := bufio.NewReader(conn)

	start := time.Now()

	// Include the file creation in the timing - it might be relevant.
	if *writeFileFlag {
		vprintf("%s Attempting to open file write for prefix '%s'",
			cinfo, *writeFilePath)
		wfdir, wfprefix := path.Split(*writeFilePath)

		var err error

		writeFile, err = ioutil.TempFile(wfdir, wfprefix)
		if err != nil {
			fmt.Fprint(os.Stderr, "%s Abort - tempfile create error: %s", cinfo, err)
			return
		}
		writeFileName = writeFile.Name()
		defer func() {
			if writeFile != nil {
				vprintf("%s Removing write file '%s'", cinfo, writeFileName)
				os.Remove(writeFileName) // Ignore errors.
			}
		}()
		vprintf("%s will write to '%s' limiting its size to %v MB",
			cinfo, writeFileName, *writeFileChunkMB)
	}

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

		if *writeFileFlag {
			if writeFileOffset+uint64(n) > writeFileSize {
				writeFileOffset = uint64(0)
				writeWrapCount++
			}
			vprintf("%s Attempt write %v bytes at offset %v", cinfo, n, writeFileOffset)
			// Re-slice to the correct size.
			writeData := data[:n]
			wn, err := writeFile.WriteAt(writeData, int64(writeFileOffset))
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"%s Abort - error writing to file '%s' at offset %v: %s\n",
					cinfo, writeFileName, writeFileOffset, err)
				return
			}
			if wn != n {
				// Short write.
				fmt.Fprintf(os.Stderr,
					"%s Abort - short write to file '%s' at offset %v (expected %v, got %v)\n",
					cinfo, writeFileName, writeFileOffset, wn, n)
				return
			}
			writeFileOffset += uint64(n)
		}

		received += uint64(n)
		vprintf("%s Received %d bytes\n", cinfo, n)
	}

	// Again, include the Close in the timing as it might matter.
	if *writeFileFlag && writeFile != nil {
		vprintf("%s Closing write file, wrap count %d", cinfo, writeWrapCount)
		writeFile.Close() // Ignore errors.
	}

	elapsed := time.Since(start)
	// Convert: bytes -> MB (divide by 10^6), s -> ns (multiply by 10^9)
	// => multiply by 10^3.
	rate := 1000.0 * float64(received) / float64(elapsed)

	iprintf("%s Received %s in %s (%.3f MBps)", cinfo, maybeHumanBytes(received), elapsed, rate)
}

func writeLoop(conn net.Conn, wg *sync.WaitGroup) {

	cinfo := fmt.Sprintf("[%s->%s] Output:", conn.RemoteAddr(), conn.LocalAddr())
	var sent uint64

	defer wg.Done()

	output := bufio.NewWriter(conn)

	start := time.Now()
WRITE:
	for {
		n, err := output.Write(netOutputData)
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

	elapsed := time.Since(start)
	// Convert: bytes -> MB (divide by 10^6), s -> ns (multiply by 10^9)
	// => multiply by 10^3.
	rate := 1000.0 * float64(sent) / float64(elapsed)

	iprintf("%s Sent %s in %s (%.3f MBps)", cinfo, maybeHumanBytes(sent), elapsed, rate)

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

func listen() {

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

func main() {
	flag.Parse()

	if !*sendFlag && !*receiveFlag {
		log.Fatalf("You must use at least one of -send and -receive!")
	}

	netOutputData = make([]byte, (*blockSizeKilobytes)*1024)

	if *listenAddr == "*" {
		*listenAddr = "0.0.0.0" // XXX Assumes IPv4.
	}

	listen() // Never returns.
}
