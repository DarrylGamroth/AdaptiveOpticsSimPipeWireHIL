/* Test peer using the original HEART Standard DM and WFS wire routines. */
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "hrtStdDm.h"
#include "hrtStdWfs.h"

int heart_dm_command(uint8_t *packet, size_t capacity, uint32_t id_frame,
		float microns)
{
	struct timespec now;
	hrtStdDm_command_datagram_t datagram = {
		.hdr = {
			.dmTargetId = 7,
			.seqNum = 1,
			.numDatagrams = 1,
			.numActVals = 1,
			.idFrame = id_frame,
		},
		.actVals = { microns },
	};
	int size = hrtStdDm_dgSize(&datagram);
	if (size <= 0 || (size_t)size > capacity || clock_gettime(CLOCK_REALTIME, &now))
		return -1;
	datagram.hdr.nsecTimestamp = (uint64_t)now.tv_sec * UINT64_C(1000000000) +
			(uint64_t)now.tv_nsec;
	if (hrtStdDm_cmdDgramChecksum(&datagram, &datagram.hdr.checksum))
		return -1;
	memcpy(packet, &datagram, (size_t)size);
	return size;
}

int heart_wfs_pixels(const uint8_t *packet, size_t size, float *pixels,
		uint16_t rows, uint16_t columns, uint16_t segment, uint32_t id_frame,
		uint64_t *timestamp)
{
	hrtStdWfs_datagram_t datagram;
	if (size < HRT_STDWFS_DATAGRAM_HEADER_SIZE || size > sizeof(datagram))
		return -1;
	memcpy(&datagram, packet, size);
	const hrtStdWfs_datagram_hdr_t *header = &datagram.hdr;
	size_t count = (size_t)rows * columns;
	if (header->networkByteOrder != 0 || header->wfsSource != 3 ||
			header->pixType != hrtStdWfs_pixType_fpCalPix ||
			header->bitsPerPixel != 32 ||
			header->numRows != rows || header->numColumns != columns ||
			header->seqDatagram != segment || header->numDatagrams != rows ||
			header->idFrame != id_frame || header->numPixels != columns ||
			header->rasterScanPosition != (size_t)(segment - 1) * columns ||
			header->rasterScanPosition + header->numPixels > count)
		return -1;
	*timestamp = header->nsecTimestamp;
	return hrtStdWfs_readValidation(size, &datagram, pixels);
}
