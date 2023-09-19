module PerfTester.Commands

open System.IO.Pipelines
open pulsar.proto
open System.IO
open ProtoBuf

type CommandType = BaseCommand.Type
[<Literal>]
let DEFAULT_MAX_MESSAGE_SIZE = 5_242_880 //5 * 1024 * 1024

let private processSimpleCommand (command : BaseCommand) (stream: Stream) (binaryWriter: BinaryWriter) (output: PipeWriter) =
    // write fake totalLength
    for i in 1..4 do
        stream.WriteByte(0uy)

    // write commandPayload
    Serializer.SerializeWithLengthPrefix(stream, command, PrefixStyle.Fixed32BigEndian)
    let frameSize = int stream.Length

    let totalSize = frameSize - 4

    //write total size and command size
    stream.Seek(0L,SeekOrigin.Begin) |> ignore
    binaryWriter.Write(int32ToBigEndian totalSize)
    stream.Seek(0L, SeekOrigin.Begin) |> ignore

    stream.CopyToAsync(output)

let serializeSimpleCommand(command : BaseCommand) =
    let f =
        fun (output: PipeWriter) ->
            backgroundTask {
                use stream = MemoryStreamManager.GetStream()
                use binaryWriter = new BinaryWriter(stream)
                return! processSimpleCommand command stream binaryWriter output
            }
    (f, command.``type``)

let newPing () : Payload =
    let response = CommandPing()
    let command = BaseCommand(``type`` = CommandType.Ping, Ping = response)
    command |> serializeSimpleCommand

let newPong () : Payload =
    let response = CommandPong()
    let command = BaseCommand(``type`` = CommandType.Pong, Pong = response)
    command |> serializeSimpleCommand

let newConnected () : Payload =
    let response = CommandConnected(
        ProtocolVersion = 15,
        ServerVersion = "PerfectTester",
        MaxMessageSize = DEFAULT_MAX_MESSAGE_SIZE
    )
    let command = BaseCommand(``type`` = CommandType.Connected, Connected = response)
    command |> serializeSimpleCommand

let newProducerSuccess requestId : Payload =
    let response = CommandProducerSuccess(
        RequestId = requestId,
        ProducerName = "PerfectTester",
        ProducerReady = true,
        SchemaVersion = [||]
    )
    let command = BaseCommand(``type`` = CommandType.ProducerSuccess, ProducerSuccess = response)
    command |> serializeSimpleCommand

let newPartitionMetadataResponse requestId : Payload =
    let response = CommandPartitionedTopicMetadataResponse(
        Partitions = 0u,
        RequestId = requestId,
        Response = CommandPartitionedTopicMetadataResponse.LookupType.Success
    )
    let command = BaseCommand(``type`` = CommandType.PartitionedMetadataResponse, partitionMetadataResponse = response)
    command |> serializeSimpleCommand

let newLookupResponse requestId : Payload =
    let response = CommandLookupTopicResponse(
        RequestId = requestId,
        Response = CommandLookupTopicResponse.LookupType.Connect,
        brokerServiceUrl = "pulsar://127.0.0.1:6650"
    )
    let command = BaseCommand(``type`` = CommandType.LookupResponse, lookupTopicResponse = response)
    command |> serializeSimpleCommand

let newSendReceipt producerId sequenceId messageId : Payload =
    let response = CommandSendReceipt(
        ProducerId = producerId,
        SequenceId = sequenceId,
        MessageId = messageId
    )
    let command = BaseCommand(``type`` = CommandType.SendReceipt, SendReceipt = response)
    command |> serializeSimpleCommand