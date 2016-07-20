import Dispatch
import Foundation

import CLibPQ
import Result


/// This library provides:
///
/// * A low-level, thread-safe interface to a single Postgres connection,
///   providing access to most Postgres functionality.
///
/// * A higher-level convenience API for working with a Postgres connection in
///   a safe way.


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

    func query(_ text: String, params: [String?] = []) throws -> Rows {
        return try request(Exec.execParams(text, params))
    }

    func transaction<T>(block: @noescape () throws -> T) rethrows -> T {
        return try with {
            let stat = PQtransactionStatus(cxn)
            guard let actions = transactionActions(stat: stat) else {
                throw Error.badConnectionStatus("Transaction: \(stat)")
            }
            do {
                _ = try request(Exec.exec(actions.begin))
                let data = try block()
                _ = try request(Exec.exec(actions.end))
                return data
            } catch let e {
                _ = try? request(Exec.exec(actions.rollback))
                throw e
            }
        }
    }


    /// Cancel whatever the connection is doing.
    func cancel() throws {
        try cancelToken.cancel()
    }

    // TODO: copy (bulk load)
//    func copy<C: Sequence, CC: Sequence
//              where CC.Iterator.Element == C, C.Iterator.Element == String?>
//        (_ rows: CC) -> [Rows] {
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
        try with {
            if PQconsumeInput(cxn) == 0 { throw Error.probe(cxn: cxn) }
        }
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

    /// Get the next batch of rows or throw if there was an error.
    func rows() throws -> Rows? {
        let data: OpaquePointer? = with {
            defer { processNotifications() }
            let ptr = PQgetResult(cxn)
            return ptr
        }
        let rows = data.map { Rows($0) }
        try rows?.check()
        return rows
    }

    /// Perform any low-level libpq operation that communicates with the
    /// server.
    func request<R: Request>(_ req: R) throws -> R.Response {
        // Send pre event here.
        return try req.call(self)
        // Send post event here.
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
public protocol Request {
    associatedtype Response

    func call(_: Connection) throws -> Response
}


/// Synchronous API.
public enum Exec: Request {
    public typealias Response = Rows

    case exec(String)
    case execParams(String, [String?])
    case prepare(String, String)
    case execPrepared(String, [String?])
    case describePrepared(String)
    case describePortal(String)

    public func call(_ conn: Connection) throws -> Rows {
        let rows = Rows(conn.with {
            defer { conn.processNotifications() }
            switch self {
            case let .exec(text):
                return PQexec(conn.cxn, text)
            case let .execParams(text, params):
                let (len, arr) = pointerize(params)
                return PQexecParams(conn.cxn, text, len, nil, arr, nil, nil, 0)
            case let .prepare(statement, text):
                return PQprepare(conn.cxn, statement, text, 0, nil)
            case let .execPrepared(s, params):
                let (len, arr) = pointerize(params)
                return PQexecPrepared(conn.cxn, s, len, arr, nil, nil, 0)
            case let .describePrepared(statement):
                return PQdescribePrepared(conn.cxn, statement)
            case let .describePortal(portal):
                return PQdescribePortal(conn.cxn, portal)
            }
        })
        try rows.check()
        return rows
    }
}


/// Async query API.
public enum Send: Request {
    public typealias Response = ()

    case sendQuery(String)
    case sendQueryParams(String, [String?])
    case sendPrepare(String, String)
    case sendQueryPrepared(String, [String?])
    case sendDescribePrepared(String)
    case sendDescribePortal(String)

    public func call(_ conn: Connection) throws {
        try conn.with {
            var result: Int32
            switch self {
            case let .sendQuery(text):
                result = PQsendQuery(conn.cxn, text)
            case let .sendQueryParams(text, params):
                let (len, arr) = pointerize(params)
                result = PQsendQueryParams(conn.cxn, text, len, nil,
                                           arr, nil, nil, 0)
            case let .sendPrepare(statement, text):
                result = PQsendPrepare(conn.cxn, statement, text, 0, nil)
            case let .sendQueryPrepared(s, params):
                let (len, arr) = pointerize(params)
                result = PQsendQueryPrepared(conn.cxn, s, len, arr,
                                             nil, nil, 0)
            case let .sendDescribePrepared(statement):
                result = PQsendDescribePrepared(conn.cxn, statement)
            case let .sendDescribePortal(portal):
                result = PQsendDescribePortal(conn.cxn, portal)
            }
            if result == 0 { throw Error.probe(cxn: conn.cxn) }
            PQsetSingleRowMode(conn.cxn)
        }
    }
}


public enum CopyPut: Request {
    public typealias Response = ()

    case putCopyData([UInt8])
    case putCopyEnd(forceFailureWith: String?)

    public func call(_ conn: Connection) throws {
        try conn.with {
            var result: Int32
            switch self {
            case let .putCopyData(bytes):
                let ptr: UnsafePointer<Int8> = UnsafePointer(bytes)
                result = PQputCopyData(conn.cxn, ptr, Int32(bytes.count))
            case let .putCopyEnd(msg):
                result = PQputCopyEnd(conn.cxn, msg)
            }
            if result == -1 { throw Error.probe(cxn: conn.cxn) }
        }
    }
}


public enum CopyGet: Request {
    public typealias Response = Status

    case getCopyData(async: Bool)

    public enum Status {
        case tryAgain
        case data([UInt8])
        case finished
    }

    public func call(_ conn: Connection) throws -> Status {
        return try conn.with {
            var asyncFlag: Int32
            switch self {
            case let .getCopyData(async): asyncFlag = async ? 1 : 0
            }
            let size = sizeof(UnsafeMutablePointer<Int8>.self)
            let buffer: UnsafeMutablePointer<UnsafeMutablePointer<Int8>?> =
                UnsafeMutablePointer(allocatingCapacity: size)
            switch PQgetCopyData(conn.cxn, buffer, asyncFlag) {
            case -2: throw Error.probe(cxn: conn.cxn)
            case -1: return Status.finished
            case 0: return Status.tryAgain
            case let n:
                if let data = buffer.pointee {
                    let ptr: UnsafePointer<UInt8> = UnsafePointer(data)
                    let buf: UnsafeBufferPointer<UInt8> =
                        UnsafeBufferPointer(start: ptr, count: Int(n))
                    return Status.data(Array(buf))
                }
                return Status.tryAgain
            }
        }
    }
}


public final class Rows {
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

    func data() -> [[String?]] {
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

    func errorField(char: Character) -> String? {
        let field = Int32((String(char).unicodeScalars.first?.value)!)
        if let data = PQresultErrorField(res, field) {
             return String(cString: data)
        }
        return nil
    }

    func check() throws {
        if let sqlState = errorField(char: "C") {
            throw Error.onRequest(err, sqlState: sqlState)
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
    /// Connection status was non-recoverable.
    case badConnectionStatus(String)

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


private func transactionActions(stat: PGTransactionStatusType)
    -> (begin: String, end: String, rollback: String)? {
    switch stat {
    case PQTRANS_IDLE:
        return (begin: "BEGIN", end: "END", rollback: "ROLLBACK")
    case PQTRANS_ACTIVE, PQTRANS_INTRANS:
        let savepoint = "savepoint_\(arc4random())"
        return (begin: "SAVEPOINT \(savepoint)",
                end: "RELEASE SAVEPOINT \(savepoint)",
                rollback: "ROLLBACK TO SAVEPOINT \(savepoint)")
    default:
        return nil
    }
}


let conn = try Connection("")
// let res: Rows = try conn.request(Exec.exec("SELECT now();"))
let res: [[String?]] = try conn.transaction {
    let row1 = try conn.query("SELECT clock_timestamp(), txid_current()")
    let row2 = try conn.query("SELECT clock_timestamp(), txid_current()")
    return row1.data() + row2.data()
}

print("Connection: \(conn.conninfo) \(conn.status().name)")
print("Rows: \(res)")

