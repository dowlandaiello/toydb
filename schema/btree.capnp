@0x8fa18e1ea99dab39;

struct KeyPointer {
  union {
    key @0 :UInt64;
    pointer @1 :Node;
  }
}

struct Node {
  union {
    internalNode @0 :BTreeInternalNode;
    leafNode @1 :BTreeLeafNode;
  }
}

struct RecordIdPointer {
  page @0 :UInt64;
  pageIdx @1 :UInt64;
}

struct BTreeInternalNode {
  keysPointers @0 :List(KeyPointer);
}

struct BTreeLeafNode {
  keys @0 :List(UInt64);
}