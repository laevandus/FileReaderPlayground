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

// MARK: -

final class FileMemoryCache {
    let byteCountLimit: Int
    
    init(byteCountLimit: Int = 5_000_000) {
        self.byteCountLimit = byteCountLimit
    }

    private var sortedItems = [StorageItem]()
    
    struct StorageItem {
        let byteRange: CountableRange<Int>
        let data: DispatchData
    }
    
    func data(for byteRange: CountableRange<Int>) -> DispatchData? {
        print(#fileID, #function, byteRange)
        
        // Find items which intersect with the search range
        guard let firstIntersectingItemIndex = sortedItems.firstIndex(where: { byteRange.intersects($0.byteRange) }) else { return nil }
        let intersectingItems = sortedItems[firstIntersectingItemIndex...].prefix(while: { byteRange.intersects($0.byteRange) })
        
        // Check if the whole range is covered
        var byteIndexesInCache = IndexSet(integersIn: byteRange)
        intersectingItems.forEach({ byteIndexesInCache.remove(integersIn: $0.byteRange) })
        guard byteIndexesInCache.isEmpty else { return nil }
        
        // All the byte indexes are cached, start copying from cached sections
        // Sections are in a sorted array, therefore just copy section by section
        guard let byteRangePointer = malloc(byteRange.count) else { return nil }
        for storageItem in intersectingItems {
            let rangeToCopyInData: CountableRange<Int> = {
                switch (byteRange.contains(storageItem.byteRange.startIndex), byteRange.contains(storageItem.byteRange.endIndex - 1)) {
                case (true, true):
                    // Everything: |+++++++++++++|
                    return 0..<storageItem.byteRange.count
                case (false, true):
                    // End: |---++++++++++|
                    return byteRange.startIndex - storageItem.byteRange.startIndex..<storageItem.byteRange.count
                case (true, false):
                    // Start: |++++++++++---|
                    return 0..<byteRange.endIndex - storageItem.byteRange.startIndex
                case (false, false):
                    fatalError("Incorrect match")
                }
            }()
            // Find the byte range in the allocated memory range where to copy the cached data section
            let destinationByteIndex = storageItem.byteRange.startIndex - byteRange.startIndex + rangeToCopyInData.startIndex
            let destinationPointer = byteRangePointer.advanced(by: destinationByteIndex)
            let destinationBufferPointer = UnsafeMutableRawBufferPointer(start: destinationPointer, count: rangeToCopyInData.count)
            storageItem.data.copyBytes(to: destinationBufferPointer, from: rangeToCopyInData)
            
            // Ignore remaining intersections because everything is copied
            if destinationByteIndex + rangeToCopyInData.count >= byteRange.endIndex {
                break
            }
        }
        let byteRangeBufferPointer = UnsafeRawBufferPointer(start: byteRangePointer, count: byteRange.count)
        return DispatchData(bytesNoCopy: byteRangeBufferPointer, deallocator: .free)
    }
    
    func set(_ data: DispatchData, byteRange: CountableRange<Int>) {
        print(#fileID, #function, byteRange, data.count)
        
        if cachedByteCount > byteCountLimit {
            evictByteRangesFurthest(from: byteRange)
        }
        
        if let insertionIndex = sortedItems.firstIndex(where: { $0.byteRange.startIndex >= byteRange.startIndex }) {
            // Ignore insertion when current or previous contain the same data
            // Possible would be to remove cached sections if the new one fully contains them
            let canDiscard: Bool = {
                guard !sortedItems[insertionIndex].byteRange.contains(byteRange) else { return true }
                guard insertionIndex > sortedItems.startIndex else { return false }
                return sortedItems[insertionIndex.advanced(by: -1)].byteRange.contains(byteRange)
            }()
            if !canDiscard {
                sortedItems.insert(StorageItem(byteRange: byteRange, data: data), at: insertionIndex)
                cachedByteCount += byteRange.count
            }
        }
        else {
            sortedItems.append(StorageItem(byteRange: byteRange, data: data))
            cachedByteCount += byteRange.count
        }
    }
    
    func removeAll() {
        cachedByteCount = 0
        sortedItems.removeAll()
    }
    
    // MARK: Tracking Cache Size
    
    private var cachedByteCount = 0
    
    private func evictByteRangesFurthest(from byteRange: CountableRange<Int>) {
        var remainingByteCountToRemove = cachedByteCount - byteCountLimit
        
        while remainingByteCountToRemove > 0 {
            guard !sortedItems.isEmpty else { break }
            // Remove either the last or the first item from the sorted list
            let firstDistance = abs(sortedItems.first!.byteRange.middleIndex - byteRange.middleIndex)
            let lastDistance = abs(sortedItems.last!.byteRange.middleIndex - byteRange.middleIndex)
            if lastDistance > firstDistance {
                remainingByteCountToRemove -= sortedItems.removeLast().byteRange.count
            }
            else {
                remainingByteCountToRemove -= sortedItems.removeFirst().byteRange.count
            }
        }
    }
}

private extension CountableRange where Bound: SignedInteger {
    func contains(_ other: Self) -> Bool {
        guard !other.isEmpty else { return false }
        return contains(other.startIndex) && contains(other.endIndex - 1)
    }
    
    var middleIndex: Bound {
        guard !isEmpty else { return startIndex }
        return (endIndex - 1 - startIndex) / 2
    }
    
    func intersects(_ other: Self) -> Bool {
        guard !other.isEmpty else { return false }
        return contains(other.startIndex) || contains(other.endIndex - 1)
    }
}


// MARK: -

func testReadFromFile() {
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
}

extension DispatchData {
    static func generateData(for range: CountableRange<Int>) -> DispatchData {
        let maxUInt8 = Int(UInt8.max)
        let numbers = range.map({ UInt8($0.quotientAndRemainder(dividingBy: maxUInt8).remainder) })
        return numbers.withUnsafeBytes({ DispatchData(bytes: $0) })
    }
}

func testMemoryCache() {
    let cache = FileMemoryCache(byteCountLimit: 1000)
    
    let ranges = [0..<15, 5..<25, 20..<35, 30..<45, 40..<50]
    ranges.forEach({ cache.set(DispatchData.generateData(for: $0), byteRange: $0) })
    
    let expectedRange = 18..<32
    let result = cache.data(for: expectedRange)
    
    assert(result?.map({ $0 }) == DispatchData.generateData(for: expectedRange).map({ $0 }))
}

testReadFromFile()
testMemoryCache()
