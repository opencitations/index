// SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
// SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
// SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
//
// SPDX-License-Identifier: ISC

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