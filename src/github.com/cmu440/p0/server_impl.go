// Implementation of a KeyValueServer.
// Students should write their code in this file.
/* 	15440 Distributed System
Project 0: Implementing a key-value messaging system
Student: Zhiqi Gong
andrew_id:zhiqig

Description:
The goal of this assignment is to get up to speed on the Go programming
language and to help remind you about the basics of socket programming that you
learned in 15-213. In this assignment you will implement a key-value database
server in Go, with a slight twist to it: The values are lists of messages which
can be retrieved with a get() query and added to with a put() query. Keys can
be completely deleted with a delete() query. You can think of this as the basis
for extremely simple online messaging system where every client is allowed to
read every other client’s messages.

*/
package p0

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
)

//Server struct
//Channels are for goroutine steps
type keyValueServer struct {
	listen               net.Listener
	Active_client        []client
	Dead_client          int
	need_to_close        chan client
	write_channel        chan get_res
	conn_channel         chan net.Conn
	put_channel          chan put_res
	get_channel          chan get_res
	need_to_get          chan client
	need_to_get_key      chan string
	del_channel          chan del_res
	close                chan int
	count_alive          chan int
	count_dead           chan int
	write_buffer_channel chan get_res
}

//put res are for read:put request
//send the res back to Main routines
//for actual handles
type put_res struct {
	key []byte
	val []byte
}

//get res are for read:get request
//send the res back to Main routines
//for actual handles
type get_res struct {
	conn net.Conn
	key  []byte
	val  []byte
}

//get res are for read: delete request
//send the res back to Main routines
//for actual handles
type del_res struct {
	key []byte
	val []byte
}

//client are designed for each client
//each client has buffer channel for writing
//This is for slow writing client task
//write_channel_buffer size is 500
type client struct {
	conn          net.Conn
	write_channel chan get_res
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	var New keyValueServer
	New.listen = nil
	New.Active_client = make([]client, 0)
	New.Dead_client = 0
	New.need_to_close = make(chan client)
	New.write_channel = make(chan get_res)
	New.conn_channel = make(chan net.Conn)
	New.put_channel = make(chan put_res)
	New.get_channel = make(chan get_res)
	New.need_to_get = make(chan client)
	New.need_to_get_key = make(chan string)
	New.del_channel = make(chan del_res)
	New.close = make(chan int)
	New.count_alive = make(chan int)
	New.count_dead = make(chan int)
	New.write_buffer_channel = make(chan get_res)
	return &New
}

//Initialize db and server
//begin listening
func (kvs *keyValueServer) Start(port int) error {
	//init a DB
	init_db()
	//start listen
	addr := ":" + strconv.Itoa(port)
	var listen_err error
	kvs.listen, listen_err = net.Listen("tcp", addr)
	if listen_err != nil {
		return listen_err
	}
	//Accept channel is open wait for
	go kvs.Accept_Routine()
	//Meanwhile we will do read and write in main routine
	go kvs.Main_Routine()
	return nil
}

//Send close signal
func (kvs *keyValueServer) Close() {
	kvs.close <- 1
}

//Send count-alive signal and
// get res from Main Routine
func (kvs *keyValueServer) CountActive() int {
	kvs.count_alive <- 1
	return <-kvs.count_alive
}

//Send count-dead signal and
// get res from Main Routine
func (kvs *keyValueServer) CountDropped() int {
	kvs.count_dead <- 1
	return <-kvs.count_dead
}

//Accpet Routine: accept and build conn
//to each clients
//send the conn to main routine
func (kvs *keyValueServer) Accept_Routine() error {
	for {
		TCP_conn, err := kvs.listen.Accept()
		if err != nil {
			return err
		}
		kvs.conn_channel <- TCP_conn
	}
}

//Main Routine: receive from other functions
//and handle all DB requests
//receive from channels
func (kvs *keyValueServer) Main_Routine() error {
	for {
		select {
		case <-kvs.close:
			kvs.listen.Close()
			for _, c := range kvs.Active_client {
				c.conn.Close()
				kvs.Dead_client++
			}
			return nil
		case <-kvs.count_alive:
			kvs.count_alive <- len(kvs.Active_client) - kvs.Dead_client
		case <-kvs.count_dead:
			kvs.count_dead <- kvs.Dead_client
		case new_conn := <-kvs.conn_channel:

			var new_client client
			new_client.conn = new_conn
			new_client.write_channel = make(chan get_res, 500)
			kvs.Active_client = append(kvs.Active_client, new_client)
			go kvs.Read_Routine(new_client)
			go kvs.Write_Routine(new_client)

		case del_res := <-kvs.del_channel:
			clear(string(del_res.key))

		case put_res := <-kvs.put_channel:
			put(string(put_res.key), put_res.val)

		case res := <-kvs.need_to_get:
			key := <-kvs.need_to_get_key
			values := get(key)
			if len(res.write_channel) < 500 {
				for _, val := range values {
					val := string(val[:len(val)-1])
					res.write_channel <- get_res{res.conn,
						[]byte(key), []byte(val)}
				}
			}

		case need_to_close := <-kvs.need_to_close:
			need_to_close.conn.Close()
			kvs.Dead_client++
		}
	}

	return nil
}

//Write: put the msg together and send it back to
// the clients
func (kvs *keyValueServer) Write_Routine(client client) error {
	for {
		select {
		case get_res := <-client.write_channel:
			val := string(get_res.val)
			key := string(get_res.key)
			res := key + "," + val + "\n"
			get_res.conn.Write([]byte(res))
		}
	}
}

//Read Routine: receive from client and parse its requests
//Do not handle request
//Send signals through channels
func (kvs *keyValueServer) Read_Routine(new_client client) error {
	//example: "put,%s,value_%d%s\n"
	TCP_conn := new_client.conn
	content := bufio.NewReader(TCP_conn)
	for {
		con, read_err := content.ReadBytes(byte('\n'))
		if read_err != nil {
			need_to_close := client{TCP_conn, nil}
			kvs.need_to_close <- need_to_close
			return read_err
		}
		c := bytes.Split(con, []byte(","))
		cmd := string(c[0])
		switch cmd {
		case "put":
			if len(c) == 3 {
				key := c[1]
				val := c[2]
				kvs.put_channel <- put_res{key, val}
			}

		case "get":
			if len(c) == 2 {
				key := string(c[1])
				key = key[:len(key)-1]
				kvs.need_to_get <- new_client
				kvs.need_to_get_key <- key
			}

		case "delete":
			if len(c) == 2 {
				key := string(c[1])
				key = key[:len(key)-1]
				kvs.del_channel <- del_res{[]byte(key), nil}
			}
		}
	}
}
