#include "parlay/sequence.h"
#include "parlay/primitives.h"
#include "parlay/utilities.h"
#include "parlay/parallel.h"

#include "kway/partitioning.h"

int main(){
    auto input = parlay::internal::tabulate(100, [&](int i){
        return i;
    });
    parlay::sequence<int> pivots(1);
    pivots[0] = 50;

    partition(input, 1, 2, pivots);
    return 0;
}