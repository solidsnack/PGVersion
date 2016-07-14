import Dispatch
import Foundation

import CLibPQ


public func libVersion() -> Int32 {
    return PQlibVersion()
}


public class Conn {
    let conninfo: URL
    private let cxn: OpaquePointer!
    private let queue =
        DispatchQueue(label: "com.github.solidsnack.DispatchPQ")

    static func connectdb(_ conninfo: String) throws -> Conn {
        let s = conninfo == "" ? "postgres:///" : conninfo
        guard let url = URL(string: s) else { throw Error.badURL }
        return try connectdb(url)
    }

    static func connectdb(_ conninfo: URL) throws -> Conn {
        let cxn = PQconnectdb(conninfo.relativeString)
        if PQstatus(cxn) != CONNECTION_OK { throw Error.probe(cxn: cxn) }
        return Conn(cxn, url: conninfo)
    }

    init(_ underlyingStruct: OpaquePointer!, url: URL) {
        cxn = underlyingStruct
        conninfo = url
    }

    deinit {
        PQfinish(cxn)
    }

    func reset() throws {
        try queue.sync {
            PQreset(cxn)
            if PQstatus(cxn) != CONNECTION_OK { throw Error.probe(cxn: cxn) }
        }
    }

    func status() -> ConnStatusType {
        return queue.sync { PQstatus(cxn) }
    }


    // Synchronous interfaces. Always casts to Result.

    private func sync(f: @noescape () -> OpaquePointer!) -> Result {
        return Result(queue.sync(execute: f))
    }

    /// Execute a query and return the results. It is possible to pass
    /// multiple independent queries in one go, although only the last result
    /// will be returned.
    /// Any errors will be found in the `Result` object.
    func exec(_ query: String) -> Result {
        return sync { PQexec(cxn, query) }
    }

    /// Execute a query with parameters. Only one query may be sent.
    /// Any errors will be found in the `Result` object.
    func exec(_ query: String, _ params: Array<String?>) -> Result {
        let (len, pointers) = pointerize(params)
        return sync {
            PQexecParams(cxn, query, len, nil, pointers, nil, nil, 0)
        }
    }

    /// Execute a prepared statement and return the results.
    /// Any errors will be found in the `Result` object.
    func exec(statement: String, _ params: Array<String?> = []) -> Result {
        let (len, pointers) = pointerize(params)
        return sync {
            PQexecPrepared(cxn, statement, len, pointers, nil, nil, 0)
        }
    }

    /// Create a preprated statement.
    /// Any errors will be found in the `Result` object.
    func prepare(statement: String, _ query: String) -> Result {
        return sync { PQprepare(cxn, statement, query, 0, nil) }
    }

    /// Describe a preprated statement.
    /// Any errors will be found in the `Result` object.
    func describe(statement: String) -> Result {
        return sync { PQdescribePrepared(cxn, statement) }
    }

    /// Describe a portal (cursor).
    /// Any errors will be found in the `Result` object.
    func describe(portal: String) -> Result {
        return sync { PQdescribePortal(cxn, portal) }
    }


    // Async interfaces. Always checks error code and throws.

    private func async(stream: Bool = true, f: @noescape () -> Int32) throws {
        try queue.sync {
            if 0 == f() { throw Error.probe(cxn: cxn) }
            if stream { PQsetSingleRowMode(cxn) }
        }
    }

    /// With `.send`, performs the query asynchronously.
    func exec(_ query: String, _ send: Send) throws {
        try async { PQsendQuery(cxn, query) }
    }

    /// With `.send`, uses the `PQsend*` (asynchronous) variant.
    func exec(_ query: String, _ params: Array<String?>, _ send: Send) throws {
        let (len, pointers) = pointerize(params)
        try async {
            PQsendQueryParams(cxn, query, len, nil, pointers, nil, nil, 0)
        }
    }

    /// With `.send`, uses the `PQsend*` (asynchronous) variant.
    func exec(statement: String, _ params: Array<String?> = [],
              _ send: Send) throws {
        let (len, pointers) = pointerize(params)
        try async {
            PQsendQueryPrepared(cxn, statement, len, pointers, nil, nil, 0)
        }
    }

    /// With `.send`, uses the `PQsend*` (asynchronous) variant.
    func prepare(statement: String, _ query: String, _ send: Send) throws {
        try async { PQsendPrepare(cxn, statement, query, 0, nil) }
    }

    /// With `.send`, uses the `PQsend*` (asynchronous) variant.
    func describe(statement: String, _ send: Send) throws {
        try async { PQsendDescribePrepared(cxn, statement) }
    }

    /// With `.send`, uses the `PQsend*` (asynchronous) variant.
    func describe(portal: String, _ send: Send) throws {
        try async { PQsendDescribePortal(cxn, portal) }
    }

    /// Return `Result` objects till we have processed everything.
    func getResult() -> Result? {
        return queue.sync {
            if let data: OpaquePointer = PQgetResult(cxn) {
                return Result(data)
            }
            return nil
        }
    }

    func consumeInput() throws {
        try async(stream: false) { PQconsumeInput(cxn) }
    }

    func busy() throws -> Bool {
        return try queue.sync {
            if 0 == PQconsumeInput(cxn) { throw Error.probe(cxn: cxn) }
            return 0 != PQisBusy(cxn)
        }
    }
}


public enum Send { case send }


class Result {
    private let res: OpaquePointer!

    private init(_ underlyingStruct: OpaquePointer) {
        res = underlyingStruct
    }

    deinit {
        PQclear(res)
    }

    var status: ExecStatusType {
        get {
            return PQresultStatus(res)
        }
    }

    var err: String {
        get {
            return String(cString: PQresultErrorMessage(res))
        }
    }

    var rows: Int32 {
        get {
            return PQntuples(res)
        }
    }
}

enum Error : ErrorProtocol {
    /// Unstructured message that is sometimes the only thing available.
    case message(String)
    /// Bad URL parse for conninfo URL.
    case badURL

    private static func probe(cxn: OpaquePointer!) -> Error {
        return Error.message(String(cString: PQerrorMessage(cxn)))
    }
}

extension ExecStatusType {
    var message: String {
        get { return String(cString: PQresStatus(self)) }
    }
}

extension ConnStatusType {
    var name: String {
        get {
            switch self {
            case CONNECTION_OK: return "CONNECTION_OK"
            case CONNECTION_BAD: return "CONNECTION_BAD"
            case CONNECTION_STARTED: return "CONNECTION_STARTED"
            case CONNECTION_MADE: return "CONNECTION_MADE"
            case CONNECTION_AWAITING_RESPONSE:
                return "CONNECTION_AWAITING_RESPONSE"
            case CONNECTION_AUTH_OK: return "CONNECTION_AUTH_OK"
            case CONNECTION_SETENV: return "CONNECTION_SETENV"
            case CONNECTION_SSL_STARTUP: return "CONNECTION_SSL_STARTUP"
            case CONNECTION_NEEDED: return "CONNECTION_NEEDED"
            default: return "CONNECTION_UNKNOWN"
            }
        }
    }
}

private func pointerize(_ params: Array<String?> = [])
        -> (Int32, Array<UnsafePointer<Int8>?>) {
    return (Int32(params.count),
            params.map({ s in s?.withCString { $0 } }))
}


let conn = try Conn.connectdb("")
let res = conn.exec("SELECT now();")

print("Connection: \(conn.conninfo) \(conn.status().name)")
print("Rows: \(res.rows)")
print("Status: \(res.status.message)")
