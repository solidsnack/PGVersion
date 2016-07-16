import Dispatch
import Foundation

import CLibPQ


public func libVersion() -> Int32 {
    return PQlibVersion()
}


public class Conn {
    let conninfo: URL

    private let cxn: OpaquePointer!
    private let queue: DispatchQueue =
        DispatchQueue(label: "com.github.solidsnack.DispatchPQ")

    private var clientThread: pthread_t? = nil
    private var cookie: Int = 0

    init(_ conninfo: URL, cxn: OpaquePointer!) {
        self.cxn = cxn
        self.conninfo = conninfo
    }

    deinit {
        PQfinish(cxn)
    }

    convenience init(_ conninfo: String) throws {
        let s = conninfo == "" ? "postgres:///" : conninfo
        guard let url = URL(string: s) else { throw Error.badURL }
        try self.init(url)
    }

    convenience init(_ conninfo: URL) throws {
        let cxn = PQconnectdb(conninfo.relativeString)
        if PQstatus(cxn) != CONNECTION_OK { throw Error.probe(cxn: cxn) }
        self.init(conninfo, cxn: cxn)
    }

    // The usual interfaces, sync and async.

    /// Synchronously perform various operations. A low-level interface.
    func exec(_ op: Op) -> Result {
        return sync {
            switch op {
            case let .query(text):
                return PQexec(cxn, text)
            case let .queryParams(text, params):
                let (len, arr) = pointerize(params)
                return PQexecParams(cxn, text, len, nil, arr, nil, nil, 0)
            case let .prepare(statement, text):
                return PQprepare(cxn, statement, text, 0, nil)
            case let .execPrepared(statement, params):
                let (len, arr) = pointerize(params)
                return PQexecPrepared(cxn, statement, len, arr, nil, nil, 0)
            case let .describePrepared(statement):
                return PQdescribePrepared(cxn, statement)
            case let .describePortal(portal):
                return PQdescribePortal(cxn, portal)
            }
        }
    }

    /// Perform a single query, optionally with paremeters, and return the
    /// result.
    func exec(_ query: String, _ params: Array<String?> = []) -> Result {
        return exec(.queryParams(query, params))
    }

    /// Perform various operations in the background. A low-level interface.
    func send(_ op: Op) throws {
        try async {
            switch op {
            case let .query(text):
                return PQsendQuery(cxn, text)
            case let .queryParams(text, params):
                let (len, arr) = pointerize(params)
                return PQsendQueryParams(cxn, text, len, nil, arr, nil, nil, 0)
            case let .prepare(statement, text):
                return PQsendPrepare(cxn, statement, text, 0, nil)
            case let .execPrepared(s, params):
                let (len, arr) = pointerize(params)
                return PQsendQueryPrepared(cxn, s, len, arr, nil, nil, 0)
            case let .describePrepared(statement):
                return PQsendDescribePrepared(cxn, statement)
            case let .describePortal(portal):
                return PQsendDescribePortal(cxn, portal)
            }
        }
    }

    /// Send a single query, optionally with params, off to be executed in the
    /// background.
    func send(_ query: String, _ params: Array<String?> = []) throws {
        try send(.queryParams(query, params))
    }

    /// Safely access the connection for multiple operations. While the
    /// closure parameter is running, no other threads may access the
    /// connection. This prevents threads from interleaving statements when
    /// running multi-step operations.
    func with<T>(block: @noescape () throws -> T) rethrows -> T {
        let me = pthread_self()
        if let t = clientThread where t == me {
            // We are in the thread and it is blocking on this code, so...
            return try block()
        } else {
            return try queue.sync {
                clientThread = me
                cookie = Date().hashValue             // TODO: Better technique
                defer { clientThread = nil }
                return try block()
            }
        }
    }


    /// Synchronous interfaces return `PGresult` (i.e., `OpaquePointer!`).
    private func sync(block: @noescape () -> OpaquePointer!) -> Result {
        return Result(with { block() })
    }

    /// Async interfaces return an error code.
    private func async(streaming: Bool = true,
                       block: @noescape () -> Int32) throws {
        try with {
            if 0 == block() { throw Error.probe(cxn: cxn) }
            if streaming { PQsetSingleRowMode(cxn) }
        }
    }

    /// Re-establishes a Postgres connection, using the existing options.
    func reset() throws {
        try with {
            PQreset(cxn)
            try check()
        }
    }

    /// Returns the connection status, safely accessing the connection to do
    /// so.
    func status() -> ConnStatusType {
        return with { PQstatus(cxn) }
    }

    /// Throws if the connection is in an unhealthy state.
    func check() throws {
        if status() != CONNECTION_OK { throw Error.probe(cxn: cxn) }
    }

    /// Retrieves buffered input from the underlying socket and stores it in a
    /// staging area. This clears any relevant select/epoll/kqueue state.
    func consumeInput() throws {
        try async(streaming: false) { PQconsumeInput(cxn) }
    }

    /// Determine if the connection is busy (processing an asynchronous query).
    func busy() throws -> Bool {
        return try with {
            try consumeInput()
            return 0 != PQisBusy(cxn)
        }
    }

    /// Return `Result` objects till we have processed everything.
    func getResult() -> Result? {
        let data: OpaquePointer? = with { PQgetResult(cxn) }
        return data.map { Result($0) }
    }
}


enum Op {
    case query(String)
    case queryParams(String, Array<String?>)
    case prepare(String, String)
    case execPrepared(String, Array<String?>)
    case describePrepared(String)
    case describePortal(String)
}


public class Result {
    private let res: OpaquePointer!

    private init(_ underlyingStruct: OpaquePointer) {
        res = underlyingStruct
    }

    private init(block: @noescape () -> OpaquePointer!) {
        res = block()
    }

    deinit {
        PQclear(res)
    }

    var status: ExecStatusType {
        return PQresultStatus(res)
    }

    var err: String {
        return String(cString: PQresultErrorMessage(res))
    }

    var rows: Int32 {
        return PQntuples(res)
    }
}

public enum Error : ErrorProtocol {
    /// Unstructured message that is sometimes the only thing available.
    case message(String)
    /// Bad URL parse for conninfo URL.
    case badURL
    case staleResultsCookie

    /// Retrieve error data from connection.
    private static func probe(cxn: OpaquePointer!) -> Error {
        return Error.message(String(cString: PQerrorMessage(cxn)))
    }

    /// Translate PQ error handling idiom to `throw`.
    private static func check(_ cxn: OpaquePointer!,
                              block: @noescape () -> Int32) throws {
        if block() != 0 { return }
        throw Error.message(String(cString: PQerrorMessage(cxn)))
    }
}

extension ExecStatusType {
    var message: String {
        return String(cString: PQresStatus(self))
    }
}

extension ConnStatusType {
    var name: String {
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
        default: return "CONNECTION_STATUS_\(self.rawValue)"
        }
    }
}

private func pointerize(_ params: Array<String?> = [])
        -> (Int32, Array<UnsafePointer<Int8>?>) {
    return (Int32(params.count),
            params.map({ s in s?.withCString { $0 } }))
}


let conn = try Conn("")
let res = conn.exec("SELECT now();")

print("Connection: \(conn.conninfo) \(conn.status().name)")
print("Rows: \(res.rows)")
print("Status: \(res.status.message)")
