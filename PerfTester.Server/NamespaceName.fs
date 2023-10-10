namespace PerfTester.Server

type NamespaceName(name : string) =

    let tenant, localName =
        name
        |> fun text -> text.Split('/')
        |> fun ar -> (ar.[0], ar.[1])

    member this.Tenant = tenant

    member this.LocalName = localName

    static member SYSTEM_NAMESPACE = NamespaceName("pulsar/system")

    override this.ToString() =
        name