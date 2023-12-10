@0xe76ff942117d4e50;

struct RecordId {
  page @0 :UInt64;
  pageIdx @1 :UInt64;
}

struct RecordIdPointer {
  isEmpty @0 :Bool;
  page @1 :UInt64;
  pageIdx @2 :UInt64;
}

struct BTreeInternalNode {
  isLeafNode @0 :Bool;
  keysPointers @1 :List(UInt64);
}

struct BTreeLeafNode {
  isLeafNode @0 :Bool;
  keys @1 :List(UInt64);
  diskPointers @2 :List(RecordIdPointer);
}

struct Record {
  size @0 :UInt64;
  data @1 :Data;
}

struct Tuple {
  relName @0 :Text;
  elements @1 :List(Data);
}

struct Page {
  spaceUsed @0 :UInt64;
  data @1 :List(Record);
}