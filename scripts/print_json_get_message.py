#!/usr/bin/env python3
import sys
import json
from pprint import pprint

def decode(elements, idx):
    element = elements[idx]
    type_ = element.get('type', 'SCALAR_NODE')
    if type_ == 'SCALAR_NODE':
        return element.get('value', '')
    elif type_ == 'MAP_NODE':
        result = {}
        idx += 1
        for _ in range(element.get('size', 0)):
            result[elements[idx].get('key', '')] = decode(elements, idx)
            idx += elements[idx].get('siblingOffset', 0)+1
        return result
    elif type_ == 'SEQUENCE_NODE':
        result = []
        idx += 1
        for _ in range(element.get('size', 0)):
            result.append(decode(elements, idx))
            idx += elements[idx].get('siblingOffset', 0)+1
        return result
    elif type_ == 'NULL_NODE':
        return None
    elif type_ == 'UNDEFINED_NODE':
        return Exception('UNDEFINED_NODE')
    else:
        raise NotImplementedError

def main():
    data = json.load(sys.stdin)
    data = decode(data.get('elements', []), 0)
    pprint(data)

if __name__ == "__main__":
    main()
