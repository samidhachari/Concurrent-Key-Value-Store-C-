#include <iostream>       // For input/output (cout, cerr)
#include <list>           // For the LRU list
#include <thread>         // For multithreading
#include <chrono>         // For time measurement
#include <random>         // For random number generation
#include <vector>         // For storing threads
#include <atomic>         // For atomic variables (thread-safe counters)
#include <fstream>        // For file I/O (persistence)
#include <sstream>        // For string streams (parsing file content)
#include <future>         // For asynchronous results from threads
#include <queue>          // For the thread pool's task queue
#include <condition_variable> // For thread synchronization in the thread pool
#include <filesystem>     // For file existence checks
#include <stdexcept>      // For exceptions
#include <cstring>        // For strerror (error message handling)
#include <iomanip>        // For formatting output (e.g., precision)
#include <getopt.h>       // For command-line argument parsing
#include <tbb/concurrent_hash_map.h> // For the concurrent hash map

// Thread Pool
class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop(false) { // Constructor: initializes the thread pool
        for (size_t i = 0; i < num_threads; ++i) { // Creates the worker threads
            workers.emplace_back([this] { // Lambda function for each worker thread
                while (true) { // Each thread runs in an infinite loop
                    std::function<void()> task; // Task to be executed
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex); // Lock the task queue
                        cv.wait(lock, [this] { return stop || !tasks.empty(); }); // Wait for a task or stop signal
                        if (stop && tasks.empty()) // If stop signal and no tasks, exit thread
                            return;
                        task = std::move(tasks.front()); // Get the next task from the queue
                        tasks.pop(); // Remove the task from the queue
                    } // Mutex is released here
                    task(); // Execute the task
                }
            });
        }
    }

    ~ThreadPool() { // Destructor: stops the threads and joins them
        {
            std::unique_lock<std::mutex> lock(queue_mutex); // Lock the queue
            stop = true; // Set the stop flag
        } // Mutex is released here
        cv.notify_all(); // Notify all waiting threads
        for (std::thread& worker : workers) // Join all worker threads
            worker.join();
    }

    template <class F, class... Args> // Enqueue a task to the thread pool
    auto enqueue(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
        using return_type = decltype(f(args...)); // Determine the return type of the function
        auto task = std::make_shared<std::packaged_task<return_type()>>( // Create a packaged task
            std::bind(std::forward<F>(f), std::forward<Args>(args)...) // Bind the function and arguments
        );
        std::future<return_type> res = task->get_future(); // Get the future for the result
        {
            std::unique_lock<std::mutex> lock(queue_mutex); // Lock the queue
            if (stop) // Check if the thread pool is stopped
                throw std::runtime_error("enqueue on stopped ThreadPool");
            tasks.push([task]() { (*task)(); }); // Push the task onto the queue
        } // Mutex is released here
        cv.notify_one(); // Notify one waiting thread
        return res; // Return the future
    }

private:
    std::vector<std::thread> workers; // Vector of worker threads
    std::queue<std::function<void()>> tasks; // Queue of tasks
    std::mutex queue_mutex; // Mutex to protect the task queue
    std::condition_variable cv; // Condition variable for thread synchronization
    bool stop; // Flag to stop the threads
};

// LRU Cache Implementation (Using TBB Concurrent Hash Map)
template <typename K, typename V>
class LRUCache {
private:
    tbb::concurrent_hash_map<K, std::pair<V, typename std::list<K>::iterator>> cache; // Concurrent hash map
    std::list<K> lru_list; // LRU list
    size_t capacity; // Cache capacity
    std::mutex lru_mutex; // Mutex to protect the LRU list
    std::atomic<size_t> hits{0}; // Atomic counter for cache hits
    std::atomic<size_t> misses{0}; // Atomic counter for cache misses

public:
    LRUCache(size_t capacity) : capacity(capacity) {}

    V get(const K& key) {
        tbb::concurrent_hash_map<K, std::pair<V, typename std::list<K>::iterator>>::accessor a; // Accessor for the hash map
        if (cache.find(a, key)) { // Find the key in the cache
            {
                std::lock_guard<std::mutex> lock(lru_mutex); // Lock the LRU list
                lru_list.erase(a->second.second); // Erase the key from its current position
                lru_list.push_front(key); // Move the key to the front of the list (most recently used)
                a->second.second = lru_list.begin(); // Update the iterator in the hash map
            } // LRU mutex released
            hits++; // Increment hit counter
            return a->second.first; // Return the value
        } else {
            misses++; // Increment miss counter
            return V{}; // Return default value if not found
        }
    }

    void put(const K& key, const V& value) {
        tbb::concurrent_hash_map<K, std::pair<V, typename std::list<K>::iterator>>::accessor a;
        {
            std::lock_guard<std::mutex> lock(lru_mutex); // Lock the LRU list
            if (cache.find(a, key)) { // If key exists
                lru_list.erase(a->second.second); // Update LRU list
                lru_list.push_front(key);
                a->second.second = lru_list.begin();
                a->second.first = value; // Update value
                return;
            }

            if (cache.size() >= capacity) { // If cache is full
                K lru_key = lru_list.back(); // Get least recently used key
                lru_list.pop_back(); // Remove from LRU list
                cache.erase(lru_key); // Remove from cache
            }

            lru_list.push_front(key); // Add new key to LRU list
            bool inserted = cache.insert(a, {key, {value, lru_list.begin()}}); // Insert into cache
            if (!inserted) {
                throw std::runtime_error("Insertion failed");
            }
        } // LRU mutex released
    }

    size_t size() const {
        return cache.size();
    }

    double getHitRatio() const {
        size_t total = hits + misses;
        return total == 0 ? 0.0 : static_cast<double>(hits) / total;
    }
};

// Key-Value Store Server (with Persistence and Exception Handling)
class KeyValueStore {
private:
    LRUCache<std::string, std::string> cache; // LRU cache instance
    std::atomic<bool> running{true}; // Flag to control the background saving thread
    std::string filename; // Filename for persistence
    std::mutex file_mutex; // Mutex to protect file access
    std::thread save_thread; // Thread for background saving
    std::atomic<bool> save_needed{false}; // Flag to indicate if a save is needed

public:
    KeyValueStore(size_t capacity, const std::string& filename) : cache(capacity), filename(filename) {
        loadFromFile(); // Load data from file on startup
        save_thread = std::thread([this] { // Start background save thread
            while (running) {
                std::this_thread::sleep_for(std::chrono::seconds(5)); // Save every 5 seconds
                if (save_needed.exchange(false)) { // Check if save is needed and reset the flag
                    saveToFile(); // Save to file
                }
            }
            saveToFile(); // Final save on exit
        });
    }

    ~KeyValueStore() {
        running = false; // Signal the background save thread to stop
        save_thread.join(); // Wait for the background save thread to finish
    }

    std::string get(const std::string& key) {
        try {
            return cache.get(key); // Get value from cache
        } catch (const std::exception& e) {
            std::cerr << "Error in get: " << e.what() << std::endl;
            return "";
        }
    }

    void put(const std::string& key, const std::string& value) {
        try {
            cache.put(key, value); // Put value into cache
            save_needed = true;    // Signal that a save is needed
        } catch (const std::exception& e) {
            std::cerr << "Error in put: " << e.what() << std::endl;
        }
    }

    void stop() {
        running = false; // Stop the server
    }

    bool isRunning() const {
        return running; // Check if the server is running
    }

private:
    void loadFromFile() {
        if (!std::filesystem::exists(filename)) return; // Check if file exists
        std::ifstream file(filename); // Open the file for reading
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) { // Read line by line
                std::istringstream iss(line); // Use stringstream to parse line
                std::string key, value;
                if (std::getline(iss, key, '|') && std::getline(iss, value)) { // Split by '|'
                    cache.put(key, value); // Put data into the cache
                }
            }
            file.close(); // Close the file
        } else {
            std::cerr << "Error opening file for loading: " << filename << std::endl;
        }
    }

    void saveToFile() {
        std::lock_guard<std::mutex> lock(file_mutex); // Lock file access
        std::string temp_filename = filename + ".tmp"; // Create temporary filename
        std::ofstream temp_file(temp_filename); // Open temporary file

        if (temp_file.is_open()) {
            for (const auto& pair : cache.cache) { // Iterate through cache
                temp_file << pair.first << "|" << pair.second.first << std::endl; // Write to file
            }
            temp_file.close(); // Close temp file

            if (std::rename(temp_filename.c_str(), filename.c_str()) != 0) { // Atomic rename
                std::cerr << "Error renaming temporary file: " << std::strerror(errno) << std::endl;
            }
        } else {
            std::cerr << "Error opening temporary file: " << temp_filename << std::endl;
        }
    }
};

// Benchmarking Function
void benchmark(KeyValueStore& store, int num_ops, int num_threads, int read_ratio, int key_size, int value_size) {
    ThreadPool pool(num_threads); // Create thread pool
    auto start = std::chrono::high_resolution_clock::now(); // Start timer
    std::atomic<int> ops_completed{0}; // Atomic counter for completed operations
    std::atomic<int> reads{0};       // Atomic counter for reads
    std::atomic<int> writes{0};      // Atomic counter for writes

    for (int i = 0; i < num_ops; ++i) { // Enqueue benchmark tasks
        pool.enqueue([&store, &ops_completed, &reads, &writes, read_ratio, key_size, value_size]() {
            std::random_device rd; // Random device
            std::mt19937 gen(rd()); // Mersenne Twister engine
            std::uniform_int_distribution<> distrib(0, num_ops); // Distribution for keys
            int key_int = distrib(gen);
            std::string key = std::string(key_size, 'a' + (key_int % 26)); // Generate key of specified size
            std::string value = std::string(value_size, '0' + (key_int % 10)); // Generate value of specified size

            if (std::uniform_int_distribution<>(0, 99)(gen) < read_ratio) { // Perform read or write
                store.get(std::to_string(distrib(gen))); // Read
                reads++; // Increment read counter
            } else {
                store.put(key, value); // Write
                writes++; // Increment write counter
            }
            ops_completed++; // Increment completed operations counter
        });
    }

    while (ops_completed < num_ops) { // Wait for all tasks to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto end = std::chrono::high_resolution_clock::now(); // Stop timer
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start); // Calculate duration
    double throughput = static_cast<double>(ops_completed) / (duration.count() / 1000.0); // Calculate throughput
    double hit_ratio = store.cache.getHitRatio(); // Get cache hit ratio

    std::cout << "Benchmark finished in " << duration.count() << "ms" << std::endl; // Print benchmark results
    std::cout << "Ops Completed: " << ops_completed << std::endl;
    std::cout << "Reads: " << reads << std::endl;
    std::cout << "Writes: " << writes << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " ops/s" << std::endl;
    std::cout << "Cache Size: " << store.cache.size() << std::endl;
    std::cout << "Cache Hit Ratio: " << std::fixed << std::setprecision(2) << (hit_ratio * 100) << "%" << std::endl;
}

int main(int argc, char* argv[]) {
    int num_ops = 1000000; // Default number of operations
    int num_threads = 8;     // Default number of threads
    int read_ratio = 50;    // Default read ratio (50%)
    int key_size = 8;       // Default key size
    int value_size = 64;    // Default value size

    int opt;
    while ((opt = getopt(argc, argv, "o:t:r:k:v:")) != -1) { // Parse command-line arguments
        switch (opt) {
            case 'o':
                num_ops = std::stoi(optarg);
                break;
            case 't':
                num_threads = std::stoi(optarg);
                break;
            case 'r':
                read_ratio = std::stoi(optarg);
                if (read_ratio < 0 || read_ratio > 100) {
                    std::cerr << "Read ratio should be between 0 and 100" << std::endl;
                    return 1;
                }
                break;
            case 'k':
                key_size = std::stoi(optarg);
                if (key_size <= 0) {
                    std::cerr << "Key size should be positive" << std::endl;
                    return 1;
                }
                break;
            case 'v':
                value_size = std::stoi(optarg);
                if (value_size <= 0) {
                    std::cerr << "Value size should be positive" << std::endl;
                    return 1;
                }
                break;
            default: // Handle invalid options
                std::cerr << "Usage: " << argv[0] << " [-o num_ops] [-t num_threads] [-r read_ratio] [-k key_size] [-v value_size]" << std::endl;
                return 1;
        }
    }

    const std::string filename = "kvstore.data"; // Filename for persistence
    KeyValueStore store(1000, filename); // Create KeyValueStore instance

    benchmark(store, num_ops, num_threads, read_ratio, key_size, value_size); // Run benchmark

    store.stop(); // Stop the server
    return 0;
}




// Input:

// The project accepts input in two ways:

// Command-line arguments: These configure the benchmark and the characteristics of the data being stored.
// Data via put operations: These are the actual key-value pairs that are stored and retrieved.
// 1. Command-line arguments (passed to main):

// These arguments control the benchmark and the data characteristics:

// -o num_ops: (Integer) The total number of operations (reads and writes) to perform during the benchmark. Default: 1,000,000.
// -t num_threads: (Integer) The number of threads to use in the benchmark's thread pool. Default: 8.
// -r read_ratio: (Integer, 0-100) The percentage of operations that should be reads. The remaining percentage will be writes. Default: 50.
// -k key_size: (Integer) The length (number of characters) of the generated keys. Default: 8.
// -v value_size: (Integer) The length (number of characters) of the generated values. Default: 64.
// Example command:

// Bash

// ./key_value_store -o 2000000 -t 16 -r 75 -k 32 -v 256
// This command would run the benchmark with:

// 2,000,000 total operations
// 16 threads
// 75% reads (25% writes)
// 32-character keys
// 256-character values
// 2. Data via put operations (passed to KeyValueStore):

// The KeyValueStore::put() method takes two string arguments:

// key: (String) The key to store the value under.
// value: (String) The data to be stored.
// Example put operations (in code):

// C++

// KeyValueStore store(1000, "mydata.txt"); // Create the store
// store.put("user123", "John Doe");       // Store a user's name
// store.put("product456", "Awesome Widget"); // Store a product description
// During the benchmark, the keys and values are generated randomly within the benchmark function, using the specified key_size and value_size. The keys are generated as strings of lowercase letters, and the values are generated as strings of digits. This ensures variety in the data being tested.

// Output:

// The code produces the following output to std::cout:

// Benchmark statistics: The benchmark() function prints these statistics after the benchmark completes:
// Benchmark finished in X ms: The total time taken for the benchmark.
// Ops Completed: Y: The total number of operations (reads + writes).
// Reads: Z: The number of read operations performed.
// Writes: W: The number of write operations performed.
// Throughput: T ops/s: The number of operations per second.
// Cache Size: C: The final number of items in the LRU cache.
// Cache Hit Ratio: H%: The percentage of get operations that found the key in the cache.
// Example benchmark output:

// Benchmark finished in 185 ms
// Ops Completed: 1000000
// Reads: 500212
// Writes: 499788
// Throughput: 5405405.41 ops/s
// Cache Size: 1000
// Cache Hit Ratio: 49.87%
// Persistence file ("kvstore.data"): The KeyValueStore saves all stored key-value pairs to a file named "kvstore.data" in the same directory as the executable. The format is one key-value pair per line, separated by a pipe symbol (|):
// Example "kvstore.data" content:

// user123|John Doe
// product456|Awesome Widget
// anotherkey|anothervalue
// ...
// Key Difference: Benchmark vs. Direct put/get:

// The benchmark generates random keys and values to simulate a workload. Its output is performance statistics.
// Direct calls to put() and get() in main() (or in other parts of your code) allow you to interact with the store directly, inserting and retrieving specific data.
// The persistence mechanism ensures that the data stored via put() (both during the benchmark and direct calls) is saved to disk and loaded when the application restarts.