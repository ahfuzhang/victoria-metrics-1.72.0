package mergeset

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type inmemoryPart struct {  //这个相当于 immutable mem table，其数据来源于 inmemoryBlock
	ph partHeader
	sb storageBlock   // 所有的time series的数据，保存在这里
	bh blockHeader  //仅仅保存头的信息
	mr metaindexRow  // 元数据信息
       //下面是临时变量
	unpackedIndexBlockBuf []byte  // 保存上方的block header序列化后的数据
	packedIndexBlockBuf   []byte  // 把 unpackedIndexBlockBuf 做 ZSTD压缩。

	unpackedMetaindexBuf []byte  // 把metaindexRow序列化后，存在这里
	packedMetaindexBuf   []byte  // 把 unpackedMetaindexBuf 进行ZSTD压缩后，存在这里

	metaindexData bytesutil.ByteBuffer  // 猜测是对应着 metaindex.bin
	indexData     bytesutil.ByteBuffer  // 猜测是对应着 index.bin文件
	itemsData     bytesutil.ByteBuffer  // 数据来自 storageBlock.itemsData
	lensData      bytesutil.ByteBuffer  // 数据来自 storageBlock.lensData
}

func (mp *inmemoryPart) Reset() {
	mp.ph.Reset()
	mp.sb.Reset()
	mp.bh.Reset()
	mp.mr.Reset()

	mp.unpackedIndexBlockBuf = mp.unpackedIndexBlockBuf[:0]
	mp.packedIndexBlockBuf = mp.packedIndexBlockBuf[:0]

	mp.unpackedMetaindexBuf = mp.unpackedMetaindexBuf[:0]
	mp.packedMetaindexBuf = mp.packedMetaindexBuf[:0]

	mp.metaindexData.Reset()
	mp.indexData.Reset()
	mp.itemsData.Reset()
	mp.lensData.Reset()
}

// Init initializes mp from ib.
func (mp *inmemoryPart) Init(ib *inmemoryBlock) {  // 把 inmemoryBlock 转换为 inmemoryPart
	mp.Reset()

	// Use the minimum possible compressLevel for compressing inmemoryPart,
	// since it will be merged into file part soon.
	compressLevel := 0
	mp.bh.firstItem, mp.bh.commonPrefix, mp.bh.itemsCount, mp.bh.marshalType = ib.MarshalUnsortedData(&mp.sb, mp.bh.firstItem[:0], mp.bh.commonPrefix[:0], compressLevel)
		// 把 inmemoryBlock 的数据序列化到 storageBlock
	mp.ph.itemsCount = uint64(len(ib.items))
	mp.ph.blocksCount = 1
	mp.ph.firstItem = append(mp.ph.firstItem[:0], ib.items[0].String(ib.data)...)  //记录第一条
	mp.ph.lastItem = append(mp.ph.lastItem[:0], ib.items[len(ib.items)-1].String(ib.data)...)  //记录最后一条
       //todo: 下面的内存拷贝值得优化
	fs.MustWriteData(&mp.itemsData, mp.sb.itemsData)  // ??? 不太明白，为什么这里要拷贝一次数据. 把storageBlock的数据，拷贝到ByteBuffer
	mp.bh.itemsBlockOffset = 0
	mp.bh.itemsBlockSize = uint32(len(mp.sb.itemsData))  // todo: 内存拷贝值得优化

	fs.MustWriteData(&mp.lensData, mp.sb.lensData)
	mp.bh.lensBlockOffset = 0
	mp.bh.lensBlockSize = uint32(len(mp.sb.lensData))  //填充blockHeader结构

	mp.unpackedIndexBlockBuf = mp.bh.Marshal(mp.unpackedIndexBlockBuf[:0])  // blockHeader 序列化
	mp.packedIndexBlockBuf = encoding.CompressZSTDLevel(mp.packedIndexBlockBuf[:0], mp.unpackedIndexBlockBuf, 0)
	fs.MustWriteData(&mp.indexData, mp.packedIndexBlockBuf)  // 把 blockHeader 压缩后，写入indexData

	mp.mr.firstItem = append(mp.mr.firstItem[:0], mp.bh.firstItem...)
	mp.mr.blockHeadersCount = 1
	mp.mr.indexBlockOffset = 0
	mp.mr.indexBlockSize = uint32(len(mp.packedIndexBlockBuf))
	mp.unpackedMetaindexBuf = mp.mr.Marshal(mp.unpackedMetaindexBuf[:0])
	mp.packedMetaindexBuf = encoding.CompressZSTDLevel(mp.packedMetaindexBuf[:0], mp.unpackedMetaindexBuf, 0)
	fs.MustWriteData(&mp.metaindexData, mp.packedMetaindexBuf)  // 把 metaindexRow 写入
}

// It is safe calling NewPart multiple times.
// It is unsafe re-using mp while the returned part is in use.
func (mp *inmemoryPart) NewPart() *part {
	ph := mp.ph
	size := mp.size()
	p, err := newPart(&ph, "", size, mp.metaindexData.NewReader(), &mp.indexData, &mp.itemsData, &mp.lensData)
	if err != nil {
		logger.Panicf("BUG: cannot create a part from inmemoryPart: %s", err)
	}
	return p
}

func (mp *inmemoryPart) size() uint64 {
	return uint64(len(mp.metaindexData.B) + len(mp.indexData.B) + len(mp.itemsData.B) + len(mp.lensData.B))
}

func getInmemoryPart() *inmemoryPart {
	select {
	case mp := <-mpPool:
		return mp
	default:
		return &inmemoryPart{}
	}
}

func putInmemoryPart(mp *inmemoryPart) {
	mp.Reset()
	select {
	case mpPool <- mp:
	default:
		// Drop mp in order to reduce memory usage.
	}
}

// Use chan instead of sync.Pool in order to reduce memory usage on systems with big number of CPU cores,
// since sync.Pool maintains per-CPU pool of inmemoryPart objects.
//
// The inmemoryPart object size can exceed 64KB, so it is better to use chan instead of sync.Pool for reducing memory usage.
var mpPool = make(chan *inmemoryPart, cgroup.AvailableCPUs())
