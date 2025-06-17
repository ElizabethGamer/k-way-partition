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

// thread: TODO
// read buffer
// (buckets) buffers of size (block)

// bucket: DONE
// write pointer

// partition(begin, end, num_workers): TODO
//   buckets[] <- scan to get bucket offsets DONE
//   parallel for num workers: TODO
//     create swap buffers and bucket buffers
//     assign primary bucket based on id
//     read from b_p write pointer into swap buffer
//     int dest = -1;

//     for all buckets, starting from b_p: TODO
//       while (!bucketdone):
//         read in from primary buckets write pointer
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
  parlay::sequence<size_t> buckets(num_buckets);
  parlay::internal::map(range, [&](long i) {
    buckets[ss.find(range[i], less)]++;
  }, 256);
  parlay::scan_inplace(buckets);

  // tabulate the write pointers
  parlay::delayed_sequence<T*> write = parlay::delayed_tabulate(num_buckets, [&](long i){
    return range.begin() + buckets[i];
  });
  
  parlay::parallel_for(0, num_workers, [&](size_t i) {
    parlay::delayed_sequence<T*> read(num_buckets * num_buckets);
    
  });
}


        
