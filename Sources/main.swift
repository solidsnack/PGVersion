import Dispatch
import Foundation

import CLibPQ
import Result


public func libVersion() -> Int32 {
    return PQlibVersion()
}


public final class Connection {
    let conninfo: URL

    private let cxn: OpaquePointer!
    private var cancelToken: Cancel
    private var notifications: [Notification] = []
    private let queue: DispatchQueue =
        DispatchQueue(label: "com.github.solidsnack.DispatchPQ")

    private var clientThread: pthread_t? = nil
    private var cookie: UInt32 = arc4random()

    init(_ conninfo: URL, cxn: OpaquePointer!) {
        self.cxn = cxn
        self.cancelToken = Cancel(PQgetCancel(cxn))
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

    func query(_ text: String, _ params: [String?] = []) throws -> Response {
        try send(.sendQueryParams(text, params))
        if let result = getResult() {
            return result
        } else {
            throw Error.internalError(
                "Query with no response should be impossible...this is a bug."
            )
        }
    }

    /// Cancel whatever the connection is doing.
    func cancel() throws {
        try cancelToken.cancel()
    }

    // TODO: copy (bulk load)
//    func copy<C: Sequence, CC: Sequence
//              where CC.Iterator.Element == C, C.Iterator.Element == String?>
//        (_ rows: CC) -> [Response] {
//
//    }

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
                cookie = arc4random()
                defer { clientThread = nil }
                return try block()
            }
        }
    }

    /// Async interfaces return an error code.
    private func async(streaming: Bool = true,
                       block: @noescape () -> Int32) throws {
        try with {
            if 0 == block() { throw Error.probe(cxn: cxn) }
            if streaming { PQsetSingleRowMode(cxn) }
        }
    }

    /// Perform various operations in the background. A low-level interface.
    func send(_ op: Send) throws {
        try async {
            switch op {
            case let .sendQuery(text):
                return PQsendQuery(cxn, text)
            case let .sendQueryParams(text, params):
                let (len, arr) = pointerize(params)
                return PQsendQueryParams(cxn, text, len, nil, arr, nil, nil, 0)
            case let .sendPrepare(statement, text):
                return PQsendPrepare(cxn, statement, text, 0, nil)
            case let .sendQueryPrepared(s, params):
                let (len, arr) = pointerize(params)
                return PQsendQueryPrepared(cxn, s, len, arr, nil, nil, 0)
            case let .sendDescribePrepared(statement):
                return PQsendDescribePrepared(cxn, statement)
            case let .sendDescribePortal(portal):
                return PQsendDescribePortal(cxn, portal)
            }
        }
    }

    /// Re-establishes a Postgres connection, using the existing options.
    func reset() throws {
        try with {
            PQreset(cxn)
            try check()
            cancelToken = Cancel(PQgetCancel(cxn))
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

    func processNotifications() {
        with {
            while let ptr: UnsafeMutablePointer<PGnotify>? = PQnotifies(cxn) {
                notifications += [Notification(ptr)]
            }
        }
    }

    /// Return `Rows` till we have processed everything.
    func getResult() -> Response? {
        let data: OpaquePointer? = with {
            let ptr = PQgetResult(cxn)
            processNotifications()                     // TODO: Background this
            return ptr
        }
        return data.map { Response($0) }
    }
}


public final class Cancel {
    private let canceller: OpaquePointer!

    private init(_ canceller: OpaquePointer!) {
        self.canceller = canceller
    }

    func cancel() throws {                                   // NB: Thread safe
        let buffer = UnsafeMutablePointer<Int8>(allocatingCapacity: 256)
        defer { free(buffer) }
        let stat = PQcancel(canceller, buffer, 256)
        if stat == 0 {
            throw Error.onConnection(String(cString: buffer))
        }
    }

    deinit {
        PQfreeCancel(canceller)
    }

}


public final class Notification {
    private let ptr: UnsafeMutablePointer<PGnotify>!

    private init(_ ptr: UnsafeMutablePointer<PGnotify>!) {
        self.ptr = ptr
    }

    var channel: String {
        return String(cString: ptr.pointee.relname)
    }

    var message: String {
        return String(cString: ptr.pointee.extra)
    }

    var pid: Int32 {
        return ptr.pointee.be_pid
    }

    deinit {
        PQfreemem(ptr)
    }
}




/// A `Request` is something we forward to the server. Each request maps to a
/// a function from `libpq`.
protocol Request { }


/// Synchronous API.
enum Exec: Request {
    case exec(String)
    case execParams(String, [String?])
    case prepare(String, String)
    case execPrepared(String, [String?])
    case describePrepared(String)
    case describePortal(String)
}


/// Async query API.
enum Send: Request {
    case sendQuery(String)
    case sendQueryParams(String, [String?])
    case sendPrepare(String, String)
    case sendQueryPrepared(String, [String?])
    case sendDescribePrepared(String)
    case sendDescribePortal(String)
}


enum CopyFrom: Request {
    case putCopyData([UInt8])
    case putCopyEnd
}


enum CopyTo: Request {
    case getCopyData

    enum Status {
        case tryAgain
        case data([UInt8])
        case finished
    }
}


public final class Response {
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

    var count: Int32 {
        return PQntuples(res)
    }

    var columns: [String] {
        return (0..<PQnfields(res)).map { String(cString: PQfname(res, $0)) }
    }

    func rows() -> [[String?]] {
        var data: [[String?]] = []
        for row in 0..<PQntuples(res) {
            var thisRow: [String?] = []
            for column in 0..<PQnfields(res) {
                if 0 != PQgetisnull(res, row, column) {
                    thisRow += [nil]
                } else {
                    // NB: Deallocation is handled by libpq.
                    let ptr = PQgetvalue(res, row, column)
                    let s: String? = String(cString: ptr!)
                    thisRow += [s]
                }
            }
            data += [thisRow]
        }
        return data
    }

    func check() throws {
        let sqlStateField = Int32(("C".unicodeScalars.first?.value)!)
        let sqlState = PQresultErrorField(res, sqlStateField)!
        if err != "" {
            throw Error.onRequest(err, sqlState: String(cString: sqlState))
        }
    }
}

public enum Error : ErrorProtocol {
    /// Unstructured message that is sometimes the only thing available.
    case onConnection(String)
    /// Bad URL parse for conninfo URL.
    case badURL
    /// Error due to request to server.
    case onRequest(String, sqlState: String)
    /// Error due to library not working right.
    case internalError(String)

    /// Retrieve error data from connection.
    private static func probe(cxn: OpaquePointer!) -> Error {
        return Error.onConnection(String(cString: PQerrorMessage(cxn)))
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


let conn = try Connection("")
let res = try conn.query("SELECT now();")

print("Connection: \(conn.conninfo) \(conn.status().name)")
print("Rows: \(res.rows())")
print("Status: \(res.status.message)")
