namespace PerfTester

open System.Reflection
open System.Collections.Generic

open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open pulsar.proto
open System
open FSharp.UMX
open System.Buffers
open System.IO
open ProtoBuf
open System.Threading
open PertTester.Proto
open System.Threading.Channels

type PulsarCommand =
    | XCommandConnect of CommandConnect
    | XCommandProducer of CommandProducer
    | XCommandSend of CommandSend
    | XCommandCloseProducer of CommandCloseProducer
    | XCommandSubscribe of CommandSubscribe
    | XCommandFlow of CommandFlow
    | XCommandAck of CommandAck
    | XCommandCloseConsumer of CommandCloseConsumer
    | XCommandPing of CommandPing


and CommandParseError =
    | IncompleteCommand
    | CorruptedCommand of exn
    | UnknownCommandType of BaseCommand.Type

and SocketMessage =
    | SocketMessageWithoutReply of Payload
    | Stop

module Cnx =
    let sendSerializedPayload (writePayload, commandType: BaseCommand.Type) (connection: SocketConnection) =
        backgroundTask {
            try
                do! connection.Output |> writePayload
                return true
            with ex ->
                Console.WriteLine(ex)
                return false
        }

    let createMailBox connection =
        let sendMb = Channel.CreateUnbounded<SocketMessage>(UnboundedChannelOptions(SingleReader = true, AllowSynchronousContinuations = true))
        do (backgroundTask {
            let mutable continueLoop = true
            while continueLoop do
                match! sendMb.Reader.ReadAsync() with
                | SocketMessageWithoutReply payload ->
                    let! _ = sendSerializedPayload payload connection
                    ()
                | SocketMessage.Stop ->
                    continueLoop <- false
            }:> Task).ContinueWith(fun t ->
                if t.IsFaulted then
                    let (Flatten ex) = t.Exception
                    Console.WriteLine(ex)
                else
                    Console.WriteLine("sendMb mailbox has stopped normally"))
        |> ignore
        sendMb

    let readMessage (reader: BinaryReader) (stream: MemoryStream) frameLength =
        reader.ReadInt16() |> int16FromBigEndian |> invalidArgIf ((<>) MagicNumber) "Invalid magicNumber" |> ignore
        let messageCheckSum  = reader.ReadInt32() |> int32FromBigEndian
        let metadataPointer = stream.Position
        let metadata = Serializer.DeserializeWithLengthPrefix<MessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
        let payloadPointer = stream.Position
        let metadataLength = payloadPointer - metadataPointer |> int
        let payloadLength = frameLength - (int payloadPointer)
        let payload = reader.ReadBytes(payloadLength)
        stream.Seek(metadataPointer, SeekOrigin.Begin) |> ignore
        let calculatedCheckSum = CRC32C.Get(0u, stream, metadataLength + payloadLength) |> int32
        if (messageCheckSum <> calculatedCheckSum) then
            Console.WriteLine("Invalid checksum. Received: {0} Calculated: {1}", messageCheckSum, calculatedCheckSum)
        (metadata, payload, messageCheckSum = calculatedCheckSum)

    let readCommand (command: BaseCommand) reader stream frameLength =
        match command.``type`` with
        | BaseCommand.Type.Connect ->
            Ok (XCommandConnect command.Connect)
        | BaseCommand.Type.Producer ->
            Ok (XCommandProducer command.Producer)
        | BaseCommand.Type.Send ->
            Ok (XCommandSend command.Send)
        | BaseCommand.Type.CloseProducer ->
            Ok (XCommandCloseProducer command.CloseProducer)
        | BaseCommand.Type.Subscribe ->
            Ok (XCommandSubscribe command.Subscribe)
        | BaseCommand.Type.Ping ->
            Ok (XCommandPing command.Ping)
        | BaseCommand.Type.Flow ->
            Ok (XCommandFlow command.Flow)
        | BaseCommand.Type.Ack ->
            Ok (XCommandAck command.Ack)
        | BaseCommand.Type.CloseConsumer ->
            Ok (XCommandCloseConsumer command.CloseConsumer)
        | unknownType ->
            Result.Error (UnknownCommandType unknownType)

    let tryParse (buffer: ReadOnlySequence<byte>) =
        let length = int buffer.Length
        if (length >= 8) then
            let array = ArrayPool.Shared.Rent length
            try
                buffer.CopyTo(Span(array))
                use stream =  new MemoryStream(array)
                use reader = new BinaryReader(stream)
                let totalength = reader.ReadInt32() |> int32FromBigEndian
                let frameLength = totalength + 4
                if (length >= frameLength) then
                    let command = Serializer.DeserializeWithLengthPrefix<BaseCommand>(stream, PrefixStyle.Fixed32BigEndian)
                    let consumed = int64 frameLength |> buffer.GetPosition
                    try
                        let wrappedCommand = readCommand command reader stream frameLength
                        wrappedCommand, consumed
                    with ex ->
                        Result.Error (CorruptedCommand ex), consumed
                else
                    Result.Error IncompleteCommand, SequencePosition()
            finally
                ArrayPool.Shared.Return array
        else
            Result.Error IncompleteCommand, SequencePosition()


    let handleCommand xcmd =
        match xcmd with
        | XCommandConnect cmd ->
            Console.WriteLine("Received Connect command")
        | XCommandSend commandSend -> failwith "todo"
        | XCommandCloseProducer commandCloseProducer -> failwith "todo"
        | XCommandSubscribe commandSubscribe -> failwith "todo"
        | XCommandFlow commandFlow -> failwith "todo"
        | XCommandAck commandAck -> failwith "todo"
        | XCommandCloseConsumer commandCloseConsumer -> failwith "todo"
        | XCommandPing commandPing -> failwith "todo"
        | XCommandProducer commandProducer -> failwith "todo"

    let readSocket (connection: SocketConnection) =
        backgroundTask {
            let mutable continueLooping = true
            let reader = connection.Input

            try
                while continueLooping do
                    let! result = reader.ReadAsync()
                    let buffer = result.Buffer
                    if result.IsCompleted then
                        Console.WriteLine("Socket was disconnected normally while reading")
                        continueLooping <- false
                    else
                        match tryParse buffer with
                        | Result.Ok xcmd, consumed ->
                            handleCommand xcmd
                            reader.AdvanceTo consumed
                        | Result.Error IncompleteCommand, _ ->
                            reader.AdvanceTo(buffer.Start, buffer.End)
                        | Result.Error (CorruptedCommand ex), consumed ->
                            Console.WriteLine(ex)
                            reader.AdvanceTo consumed
                        | Result.Error (UnknownCommandType unknownType), consumed ->
                            Console.WriteLine("UnknownCommandType {0}, ignoring message", unknownType)
                            reader.AdvanceTo consumed
            with Flatten ex ->
                Console.WriteLine(ex)

            Console.WriteLine("readSocket stopped")
        } :> Task

