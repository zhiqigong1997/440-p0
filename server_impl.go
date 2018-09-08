// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"fmt"
	"net"
	"strconv"
	"bufio"
)


type keyValueServer struct {
	// TODO: implement this!
	listen        net.Listener
	count         int
	read_channel  chan []byte
	write_channel chan [][]byte
	conn_channel  chan net.Conn
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	var New keyValueServer
	New.listen=nil
	New.count=0
	New.read_channel = make(chan []byte)
	New.write_channel = make(chan [][]byte)
	New.conn_channel =  make(chan net.Conn)
	return &New
}

func (kvs *keyValueServer) Start(port int) error {
	//init a keyvalue map
	init_db()

	//start listen
	addr := "localhost:" + strconv.Itoa(port)
	kvs.listen, err = net.Listen("tcp", addr)

	//if listen return error
	if listen_err!=nil{
		return listen_err
	}
	//if listen is successful
	//Accpet channel is open wait for
 	go Accept_Routine(kvs)
	//Meawhile we will do read and write in main routine
	go Main_Routine(kvs)
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) Accept_Routine() error{
	for{
		TCP_conn,err:=kvs.listen.Accept()
		if err!=nil{
			return err
		}
		//read from TCPconn
		kvs.conn_channel<-TCP_conn
		//conn_parse(TCP_conn)
	}
}


// TODO: add additional methods/functions below!
func(kvs *keyValueServer) conn_parse(TCP_conn net.Conn) error{
	//example: "put,%s,value_%d%s\n"
	content,read_err := bufio.NewReader(TCP_conn).ReadString('\n')
	if read_err!=nil{
		return read_err
	}
	c   := content.Split(",")
	cmd := c[0]
	switch cmd{
	//case: content starts with put
	case "put":
		if len(c)==3 {
			key := c[1]
			val := []byte(c[2])
			put(key,val)


		}
	//case: content starts with get
	case "get":
		if len(c)!=2 {
			key := c[1]

			val := get(key)
			kvs.write_channel<-val

		}
	return err

	}

}
func (kvs *keyValueServer) Main_Routine() error {
	for {
		select {
		case (target <- kvs.conn_channel)

		if {

			conn_parse(target)
		}
		if{

		}
		}
	}

	return nil
}
func (kvs *keyValueServer) Write_Routine() error{
	for {
		select {

		}
	}
	res := <-kvs.write_channel

}

func (kvs *keyValueServer) Read_Routine() error{

}