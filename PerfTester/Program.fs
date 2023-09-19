// For more information see https://aka.ms/fsharp-console-apps

open System
open System.Net
open System.Net.Sockets
open Pipelines.Sockets.Unofficial
open PerfTester


[<EntryPoint>]
let main argv =

    Console.WriteLine "Starting server"

    let t =
        task {
            // Establish the local endpoint for the socket
            let localEndPoint = IPEndPoint(IPAddress.Any, 6650)

            // Create a TCP/IP socket
            use socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            // Bind the socket to the local endpoint and listen
            socket.Blocking <- false
            socket.Bind(localEndPoint)
            socket.Listen(100)
            use! startedSocket = socket.AcceptAsync()
            Console.WriteLine "Connection accepted"

            // Start an asynchronous socket to listen for connections
            use connection = SocketConnection.Create(startedSocket)
            do! Cnx.readSocket connection

        }
    t.GetAwaiter().GetResult()
    0