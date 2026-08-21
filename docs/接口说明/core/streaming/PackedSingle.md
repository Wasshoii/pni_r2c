# PackedSingle

> #include<core/streaming/PackedSingle.hpp>

## 目的

确认 `openpni::Single` 为 16 字节 pack(1)，并提供 pack/unpack 的 `memcpy` 封装。R2S 输出、RDMA 槽 payload、历史 gRPC 二进制传输共用这一布局。字段语义见 libpni [CommonDataType](../../../../../pni-standard-project/docs/接口说明/core/CommonDataType.md)。

通路位置见 [R2S到RDMA](../../通路/R2S到RDMA.md)。

命名空间：`openpni::distributed::streaming`。

## 核心接口

```cpp
static_assert(sizeof(openpni::Single) == 16, ...);
constexpr size_t kPackedSingleSize = 16;

void packSinglesToBinary(const openpni::Single *src, size_t count, void *dst);
void unpackBinaryToSingles(const void *src, size_t count, openpni::Single *dst);
```

- 两个函数都是 `memcpy(dst, src, count * 16)`，无字节序转换（约定 little-endian，与本机 PET 热路径一致）。
- `count==0` 时 memcpy 长度为 0，指针仍须非空或实现定义为可接受空拷贝（调用方应避免空指针 + 非零 count）。

## 使用提示

- 不要改 `Single` 字段顺序或类型而不改 [SlotProtocol](../../dataplane/rdma/SlotProtocol.md) 的 `kPackedSingleBytes` 与 `static_assert`。
- RDMA 发送路径在 `RdmaWriteSender::sendPackedSingles` 中按同一 16 字节步进写入 slot payload；本头文件是契约说明，不是额外编解码层。
