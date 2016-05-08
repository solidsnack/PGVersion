import Foundation

import CLibPQ


let version = PQlibVersion()

print("Version: \(version)")


typealias PGconn = OpaquePointer!
typealias PGresult = OpaquePointer!


public class CXN: PG {
    let cxn: PGconn
    let lk = NSRecursiveLock()
    var tx = false

    init(_ conninfo: String) {
        cxn = PQconnectdb(conninfo)
    }

    internal func direct(_ query: String,
                         params: Array<String?> = []) -> PGresult {
        let pointers = params.map { s in s?.withCString { $0 } }
        let len = Int32(pointers.count)
        return PQexecParams(cxn, query, len, nil, pointers, nil, nil, 0)
    }

    func safely<T>(_ task: @noescape (PG) throws -> T) rethrows -> T {
        lk.lock()
        defer { lk.unlock() }
        return try task(self)
    }

    func exec<T>(_ task: @noescape (PG) throws -> T) rethrows -> T {
        return try safely { pg in
            if !tx {
                tx = true
                direct("BEGIN;")
                defer { direct("END;") }
            }
            return try task(pg)
        }
    }

    func exec(_ s: String) -> Result {
        return safely { _ in Result(direct(s)) }
    }
}


protocol PG {
    func exec(_ query: String) -> Result
}


class Result {
    private let res: PGresult

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

    private init(_ res: PGresult) {
        self.res = res
    }

    deinit {
        PQclear(res)
    }
}

extension ExecStatusType {
    var message: String {
        get {
            return String(cString: PQresStatus(self))
        }
    }
}


let cxn = CXN("")
let res = cxn.exec("SELECT now();")

print("Rows: \(res.rows)")
print("Status: \(res.status.message)")
print("CXN: \(PQstatus(cxn.cxn))")
print("CONNECTION_OK: \(CONNECTION_OK)")
print("CONNECTION_BAD: \(CONNECTION_BAD)")
