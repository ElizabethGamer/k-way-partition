#include "parlay/sequence.h"
#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "parlay/utilities.h"
#include "heaptree.h"

// global: DONE
// bucket offsets
// elements per block
// number of buckets
// pivots

// thread: DONE
// read buffer
// (buckets) buffers of size (block)

// bucket: DONE
// write pointer

// partition(begin, end, num_workers): TODO
//   buckets[] <- scan to get bucket offsets DONE
//   parallel for num workers: TODO
//     create swap buffers and bucket buffers DONE
//     assign primary bucket based on id DONE
//     read from b_p write pointer into swap buffer
//     int dest = -1;

//     for all buckets, starting from b_p: TODO
//       while (!bucketdone):
//         read in from primary buckets read pointer and increment
//         for element in swap buffer: use queue? can we do that efficiently?
//           dest = classify(element)
//           write element to bucket_buffer[dest]
//           if (bucket_buffer[dest] is full) then increment buckets[dest]
//             also, if buckets[dest] was not empty then read in the old one into swap buffer
//             write to old buckets[dest]
//             how can we avoid the branch misprediction here?
//           maybe we can just write it directly where the bucket offsets are, to avoid cleanup
      
//       end of cycle; now we move on to next bucket
//       invariant: everything between start of bucket and write pointer is either correct or empty

template<typename T, typename compare>
void partition(parlay::sequence<T> range, size_t num_workers, compare comp, size_t num_buckets, parlay::sequence<int>& pivots){
  int n = range.size();
  int b = 5000; // elements per block; can change later

  // find the counts for each bucket, then find their offsets
  heap_tree ss(pivots); // only works for perfectly balanced trees; find alternative later
  parlay::sequence<size_t> offsets(num_buckets);
  parlay::internal::map(range, [&](long i) {
    offsets[ss.find(range[i], less)]++;
  }, 256);
  parlay::scan_inplace(offsets);

  // tabulate the write and read pointers per bucket
  parlay::delayed_sequence<T*> write = parlay::delayed_tabulate(num_buckets, [&](long i){
    return range.begin() + offsets[i] + i; // accounting for the space for pivots
  });
  auto read = parlay::tabulate(num_buckets, [&](long i){
      i == num_buckets? return range.end() - 1 : return &buckets[i + 1][0] - 2;
  });
  
  parlay::parallel_for(0, num_workers, [&](long i) {
    // swap and bucket buffers
    // can't figure out what data structure would be good. would appreciate advice/optimization
    parlay::delayed_sequence<T> swap(num_buckets * (b + 1));
    auto bucket_buffers = parlay::tabulate(num_buckets, [&](long i) {
      return parlay::delayed_sequence<T>(b);
    });

    // current positions within buffers
    // can probably do this more efficiently, but for now...
    int swapr = 0;
    int swapw = 0;
    auto bucketsw = parlay::tabulate(num_buckets, [&](long i){
      return 0;
    });

    int curr_bucket = i * num_buckets / num_workers; // for now assuming that buckets > workers
    T* dest = r


    
  });
}
