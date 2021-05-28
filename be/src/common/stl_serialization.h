// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <fstream>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <vector>

namespace doris {

template<class T>
class Serialization : public T {
public:

    void serialization(std::ofstream& ostream) {
        boost::archive::binary_oarchive oa(ostream);
        oa << *this;
    }

    void unserialization(std::ifstream& istream) {
        boost::archive::binary_iarchive ia(istream);
        ia >> *this;
    }

private:
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::base_object<T>(*this);
    }
};

template<class T>
class VectorSerialization : public Serialization<std::vector<T>> {
};

} // namespace doris
