import Dispatch
import Foundation

import CLibPQ


func libVersion() -> Int32 {
    return PQlibVersion()
}


public class Conn {
    let conninfo: URL
    private let cxn: OpaquePointer!
    private var queue =
        DispatchQueue(label: "com.github.solidsnack.DispatchPQ")

    static func connectdb(_ conninfo: String) throws -> Conn {
        guard let url = URL(string: conninfo) else { throw Error.badURL }
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

    // Synchronous interfaces. Always casts to Result.

    private func sync(f: @noescape () -> OpaquePointer!) -> Result {
        return Result(queue.sync(execute: f))
    }

    func exec(_ query: String) -> Result {
        return sync { PQexec(cxn, query) }
    }

    func exec(_ query: String, params: Array<String?>) -> Result {
        let (len, pointers) = pointerize(params)
        return sync {
            PQexecParams(cxn, query, len, nil, pointers, nil, nil, 0)
        }
    }

    func exec(statement: String, params: Array<String?> = []) -> Result {
        let (len, pointers) = pointerize(params)
        return sync {
            PQexecPrepared(cxn, statement, len, pointers, nil, nil, 0)
        }
    }

    func exec(prepare: String, query: String) -> Result {
        return sync { PQprepare(cxn, prepare, query, 0, nil) }
    }

    func exec(describe: String) -> Result {
        return sync { PQdescribePrepared(cxn, describe) }
    }

    // Async interfaces. Always checks error code and throws.

    private func async(stream: Bool = true, f: @noescape () -> Int32) throws {
        try queue.sync {
            if 0 == f() { throw Error.probe(cxn: cxn) }
            if stream { PQsetSingleRowMode(cxn) }
        }
    }

    func send(_ query: String) throws {
        try async { PQsendQuery(cxn, query) }
    }

    func send(_ query: String, params: Array<String?>) throws {
        let (len, pointers) = pointerize(params)
        try async {
            PQsendQueryParams(cxn, query, len, nil, pointers, nil, nil, 0)
        }
    }

    func send(statement: String, params: Array<String?> = []) throws {
        let (len, pointers) = pointerize(params)
        try async {
            PQsendQueryPrepared(cxn, statement, len, pointers, nil, nil, 0)
        }
    }

    func send(prepare: String, query: String) throws {
        try async { PQsendPrepare(cxn, prepare, query, 0, nil) }
    }

    func send(describe: String) throws {
        try async { PQsendDescribePrepared(cxn, describe) }
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

//let cxn = CXN("")
///// let res = cxn.exec("SELECT now();")
//
//print("Rows: \(res.rows)")
//print("Status: \(res.status.message)")
//print("CXN: \(PQstatus(cxn.cxn))")
//print("CONNECTION_OK: \(CONNECTION_OK)")
//print("CONNECTION_BAD: \(CONNECTION_BAD)")


//internal func direct(_ query: String,
//                     params: Array<String?> = []) -> PGresult {
//    let pointers = params.map { s in s?.withCString { $0 } }
//    let len = Int32(pointers.count)
//    return PQexecParams(cxn, query, len, nil, pointers, nil, nil, 0)
//    PQexec
//}
//
//
//func exec<T>(_ task: @noescape (PG) throws -> T) rethrows -> T {
//    return try safely { pg in
//        if !tx {
//            tx = true
//            _ = direct("BEGIN;")
//            defer { _ = direct("END;") }
//        }
//        return try task(pg)
//    }
//}
//
//func exec(_ s: String) -> Result {
//    return safely { _ in Result(direct(s)) }
//}
//
//protocol PG {
//    func exec(_ query: String) -> Result
//}
//
//

