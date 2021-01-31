import Foundation

final class FileReader {
    let fileURL: URL
    
    init(fileURL: URL) {
        self.fileURL = fileURL
    }
    
    deinit {
        close()
    }
    
    // MARK: Managing I/O

    private var channel: DispatchIO?
    
    func open() -> Bool {
        guard channel == nil else { return true }
        guard let path = (fileURL.path as NSString).utf8String else { return false }
        channel = DispatchIO(type: .random, path: path, oflag: 0, mode: 0, queue: .main, cleanupHandler: { error in
            print("Closed a channel with status: \(error)")
        })
        // Load the whole requested byte range at once
        channel?.setLimit(lowWater: .max)
        guard self.channel != nil else { return false }
        print("Opened a channel at \(fileURL)")
        return true
    }
    
    func close() {
        channel?.close()
        channel = nil
    }
    
    // MARK: Reading the File
    
    func read(byteRange: CountableRange<Int>, queue: DispatchQueue = .main, completionHandler: @escaping (DispatchData?) -> Void) {
        if let channel = channel {
            channel.read(offset: off_t(byteRange.startIndex), length: byteRange.count, queue: queue, ioHandler: { done, data, error in
                print(done, data?.count ?? -1, error)
                completionHandler(data)
            })
        }
        else {
            print("Channel is closed")
            completionHandler(nil)
        }
    }
}

let fileURL = Bundle.main.url(forResource: "DataFile", withExtension: nil)!
let reader = FileReader(fileURL: fileURL)
if reader.open() {
    reader.read(byteRange: 0..<20) { data in
        if let data = data {
            print("Read bytes: \(data.map({ UInt8($0) }))")
        }
        else {
            print("Failed to read data")
        }
    }
}
else {
    print("Failed to open")
}
