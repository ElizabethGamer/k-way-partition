#include "parlay/sequence.h"
#include "parlay/primitives.h"
#include "parlay/utilities.h"
#include "parlay/parallel.h"

#include "kway/partitioning.h"

int main(){
    auto input = parlay::internal::tabulate(100, [&](long i){
        return i;
    });
    return 0;
}