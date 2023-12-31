namespace PerfTester.Server

open System.IO.Pipelines

open System.Threading.Tasks
open Pipelines.Sockets.Unofficial
open pulsar.proto
open System
open System.Buffers
open ProtoBuf
open System.Threading.Channels

type PulsarCommand =
    | XCommandConnect of CommandConnect
    | XCommandProducer of CommandProducer
    | XCommandPartitionedTopicMetadata of CommandPartitionedTopicMetadata
    | XCommandLookup of CommandLookupTopic
    | XCommandSend of CommandSend
    | XCommandSubscribe of CommandSubscribe
    | XCommandFlow of CommandFlow
    | XCommandAck of CommandAck
    | XCommandPing of CommandPing
    | XCommandPong of CommandPong


and CommandParseError =
    | IncompleteCommand
    | CorruptedCommand of exn
    | UnknownCommandType of BaseCommand.Type

and SocketMessage =
    | SocketMessageWithoutReply of Payload
    | Stop

module Cnx =
    let sendSerializedPayload (writePayload: PipeWriter -> Task, commandType: BaseCommand.Type) (connection: SocketConnection) =
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

    let readCommand (command: BaseCommand) =
        match command.``type`` with
        | BaseCommand.Type.Connect ->
            Ok (XCommandConnect command.Connect)
        | BaseCommand.Type.Producer ->
            Ok (XCommandProducer command.Producer)
        | BaseCommand.Type.Send ->
            Ok (XCommandSend command.Send)
        | BaseCommand.Type.Subscribe ->
            Ok (XCommandSubscribe command.Subscribe)
        | BaseCommand.Type.Ping ->
            Ok (XCommandPing command.Ping)
        | BaseCommand.Type.Pong ->
            Ok (XCommandPong command.Pong)
        | BaseCommand.Type.Flow ->
            Ok (XCommandFlow command.Flow)
        | BaseCommand.Type.Ack ->
            Ok (XCommandAck command.Ack)
        | BaseCommand.Type.PartitionedMetadata ->
            Ok (XCommandPartitionedTopicMetadata command.partitionMetadata)
        | BaseCommand.Type.Lookup ->
            Ok (XCommandLookup command.lookupTopic)
        | unknownType ->
            Result.Error (UnknownCommandType unknownType)

    let tryParse (buffer: ReadOnlySequence<byte>) =
        let length = int buffer.Length // at least 8
        let mutable reader = SequenceReader<byte>(buffer)
        let mutable totalLength = -1
        reader.TryReadBigEndian(&totalLength) |> ignore
        let frameLength = totalLength + 4
        if (length >= frameLength) then
            let mutable baseCommandLength = -1
            reader.TryReadBigEndian(&baseCommandLength) |> ignore
            let command = Serializer.Deserialize<BaseCommand>(buffer.Slice(reader.Consumed, baseCommandLength))
            reader.Advance baseCommandLength
            let consumed = int64 frameLength |> buffer.GetPosition
            try
                let wrappedCommand = readCommand command
                wrappedCommand, consumed
            with ex ->
                Result.Error (CorruptedCommand ex), consumed
        else
            Result.Error IncompleteCommand, SequencePosition()

    let handleFlowCommand (commandFlow: CommandFlow) (mb: Channel<SocketMessage>) =
        let batchSize = 174
        for _ in 1u .. (commandFlow.messagePermits / (uint batchSize) + 1u) do
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newMessage commandFlow.ConsumerId batchSize)
            mb.Writer.TryWrite(msg) |> ignore
        ()

    let handleCommand xcmd (mb: Channel<SocketMessage>) =
        match xcmd with
        | XCommandConnect cmd ->
            Console.WriteLine("Received Connect command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newConnected())
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandProducer commandProducer ->
            Console.WriteLine("Received Producer command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newProducerSuccess(commandProducer.RequestId))
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandPing commandPing ->
            Console.WriteLine("Received Ping command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newPong())
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandPong commandPong ->
            Console.WriteLine("Received Pong command")
        | XCommandPartitionedTopicMetadata commandPartitionedTopicMetadata ->
            Console.WriteLine("Received PartitionedTopicMetadata command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newPartitionMetadataResponse(commandPartitionedTopicMetadata.RequestId))
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandLookup commandLookup ->
            Console.WriteLine("Received CommandLookup command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newLookupResponse(commandLookup.RequestId))
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandSend commandSend ->
            let messageId = MessageIdData()
            let msg = SocketMessage.SocketMessageWithoutReply (
                Commands.newSendReceipt commandSend.ProducerId commandSend.SequenceId messageId
            )
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandSubscribe commandSubscribe ->
            Console.WriteLine("Received CommandSubscribe command")
            let msg = SocketMessage.SocketMessageWithoutReply (Commands.newSuccess(commandSubscribe.RequestId))
            mb.Writer.TryWrite(msg) |> ignore
        | XCommandFlow commandFlow ->
            handleFlowCommand commandFlow mb
        | XCommandAck _ ->
            ()

    let readSocket (connection: SocketConnection) =
        task {
            let mutable continueLooping = true
            let reader = connection.Input
            let mb = createMailBox connection

            try
                while continueLooping do
                    let! result = reader.ReadAtLeastAsync(8)
                    if result.IsCompleted then
                        Console.WriteLine("Socket was disconnected normally while reading")
                        continueLooping <- false
                    else
                        let buffer = result.Buffer
                        match tryParse buffer with
                        | Result.Ok xcmd, consumed ->
                            handleCommand xcmd mb
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

