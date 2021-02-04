#ifndef MHCONFIG__CONSTANTS_H
#define MHCONFIG__CONSTANTS_H

#include <string>

namespace mhconfig
{

const static std::string TAG_NON_PLAIN_SCALAR{"!"};
const static std::string TAG_PLAIN_SCALAR{"?"};
const static std::string TAG_NONE{"tag:yaml.org,2002:null"};
const static std::string TAG_STR{"tag:yaml.org,2002:str"};
const static std::string TAG_BIN{"tag:yaml.org,2002:binary"};
const static std::string TAG_INT{"tag:yaml.org,2002:int"};
const static std::string TAG_DOUBLE{"tag:yaml.org,2002:float"};
const static std::string TAG_BOOL{"tag:yaml.org,2002:bool"};

const static std::string TAG_FORMAT{"!format"};
const static std::string TAG_SREF{"!sref"};
const static std::string TAG_REF{"!ref"};
const static std::string TAG_DELETE{"!delete"};
const static std::string TAG_OVERRIDE{"!override"};

const static uint8_t NUMBER_OF_MC_GENERATIONS{3};

const static std::string DOCUMENT_NAME_TOKENS{"tokens"};
const static std::string DOCUMENT_NAME_POLICY{"policy"};

//TODO Review the names

typedef uint16_t DocumentId;
typedef uint16_t RawConfigId;
typedef uint16_t VersionId;

} /* mhconfig */

#endif
