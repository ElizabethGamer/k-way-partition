#include "parlay/sequence.h"
#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "parlay/utilities.h"
#include "heaptree.h"

#include <atomic>

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
//     read from b_p write pointer into swap buffer DONE

//     for all buckets, starting from b_p: TODO
//       while (!bucketdone):
//         read in from primary buckets read pointer and increment DONE
//         for element in swap buffer: use queue? can we do that efficiently?
//           dest = classify(element) DONE 
//           write element to bucket_buffer[dest] DONE
//           if (bucket_buffer[dest] is full) then increment buckets[dest] DONE
//             also, if buckets[dest] was not empty then read in the old one into swap buffer
//             write to old buckets[dest] DONE
//             how can we avoid the branch misprediction here?
//           maybe we can just write it directly where the bucket offsets are, to avoid cleanup
      
//       end of cycle; now we move on to next bucket
//       invariant: everything between start of bucket and write pointer is correct

template<typename T>
int readBlock(T* src, T* dest, size_t amt, int dest_offset, size_t dest_size){
  std::move(src, src + std::min(amt, dest_size - dest_offset), std::move(dest + dest_offset));
  if (dest_size - dest_offset - amt < 0){ // wraparound; see if we can do this without branches
    std::move(src + dest_size - dest_offset, src + amt, std::move(dest));
  }

  return (dest_offset + amt) % dest_size;
}

template<typename T>
void partition(parlay::sequence<T> range, size_t num_workers, size_t num_buckets, parlay::sequence<int>& pivots){
  int n = range.size();
  int b = 5000; // elements per block; can change later

  // find the counts for each bucket, then find their offsets
  heap_tree ss(pivots); // only works for perfectly balanced trees; find alternative later
  parlay::sequence<size_t> offsets(num_buckets);
  parlay::internal::map(range, [&](long i) {
    offsets[ss.find(range[i], std::less<int>())]++;
    return i;
  }, 256);
  parlay::scan_inplace(offsets);

  // tabulate the write and read pointers per bucket
  auto write = parlay::delayed_tabulate(num_buckets, [&](long i){
    return range.begin() + offsets[i] + i; // accounting for the space for pivots
  });
  auto read = parlay::tabulate(num_buckets, [&](long i){
      return i == num_buckets? range.end() - 1 : write[i + 1] - 2;
  });
  
  parlay::parallel_for(0, num_workers, [&](long i) {
    // swap and bucket buffers
    // can't figure out what data structure would be good. would appreciate advice/optimization
    int buffersize = num_buckets * (b+ 1);
    parlay::sequence<T> swap(buffersize);
    auto bucket_buffers = parlay::tabulate(num_buckets, [&](long i) {
      return parlay::sequence<T>(b);
    });

    // current positions within buffers
    // can probably do this more efficiently, but for now...
    int swapr = 0;
    int swapw = 0;
    auto bucketsw = parlay::tabulate(num_buckets, [&](long i){
      return 0;
    });

    // assign primary bucket
    int curr_bucket = i * num_buckets / num_workers; // for now assuming that buckets > workers

    // go through all buckets
    for (int i = 0; i < num_buckets; i++){
      while (read[curr_bucket] - write[curr_bucket] >= 0){ // get rid of the race condition here later
        int amt = std::min(b, read[curr_bucket] - write[curr_bucket] + 1); // in case there's less than b elements left
        T* src = read[curr_bucket].std::atomic<T>::fetch_add(-amt);
        src -= (amt - 1); // access the start of the block to hopefully bring it into cache at once

        // read from block to buffer
        swapw = readBlock(src, &(swap[0]), amt, swapw, buffersize);

        // empty out the swap buffer
        while (swapr != swapw){ // is it possible to reduce the comparisons here?
          int dest_bucket = ss.find(swap[swapr], std::less<int>());
          std::move(&(swap[swapr]), &(bucket_buffers[dest_bucket][bucketsw[dest_bucket]]));

          // if bucket buffer full, flush
          // try to do this w/out branch
          if (bucketsw[dest_bucket] == b){
            T* dest = write[dest_bucket].std::atomic<T>::fetch_add(b);

            // write to destination
            if (dest - read[dest_bucket] > 0) { // empty space; can just write
              std::move(&bucket_buffers[dest_bucket][0], &bucket_buffers[dest_bucket][0] + b, dest);
            } else { // need to process the elements in dest first
              // read dest into swap buffer
              swapw = readBlock(dest, &swap[0], b, swapw, buffersize);
              std::move(&bucket_buffers[dest_bucket][0], &bucket_buffers[dest_bucket][0] + b, dest); // duplicate

            } // later make sure that if bucket write == read then we're doing this atomically
            bucketsw[dest_bucket] = 0;
          } else {
            bucketsw[dest_bucket]++;
          }

          // next element in swap buffer
          swapr++;
        }
        

      }

      // move on to next bucket
      curr_bucket += 1;
      curr_bucket %= num_buckets;
    }

    
  });
}
