#pragma once
#include <iostream>

using namespace std;

class StringHasher
{
public:
    // the class should have operator () with this signature :
    uint64_t operator()(string key, uint64_t seed = 0) const
    {
        uint64_t hash = hash_fn(key);
        hash ^= seed;
        return hash;
    }

    hash<string> hash_fn;
};