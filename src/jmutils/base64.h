#ifndef JMUTILS__BASE64_H
#define JMUTILS__BASE64_H

#include <cstdint>
#include <string>

namespace jmutils {

bool base64_sanitize(std::string& value);
void base64_decode(const std::string& input, std::string& out);

}

#endif
